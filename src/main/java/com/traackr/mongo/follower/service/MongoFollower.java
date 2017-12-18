/**
 * MIT License
 *
 * Copyright (c) 2017 Traackr, Inc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *   SOFTWARE.
 */
package com.traackr.mongo.follower.service;

import com.traackr.mongo.follower.model.OpLogTailerParams;
import com.traackr.mongo.follower.model.OplogEntry;
import com.traackr.mongo.follower.model.Record;

import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tails the oplog dumping each record into a blocking oplogSink for another thread
 * to process.
 *
 * @author wwinder
 * Created on: 5/25/16
 */
public class MongoFollower implements Runnable {
  private static final Logger logger = Logger.getLogger(MongoFollower.class.getName());
  static final Bson NATURAL_SORT_FOR_COLLECTION_SCAN = new BasicDBObject("$natural", new Integer(1));
  static final String OPLOG_NAMESPACE = "oplog.rs";
  static final String OPLOG_NOOP_MESSAGE ="periodic noop";
  
  final private OpLogTailerParams params;
  private MongoClient client;

  /**
   * Create an oplog iterator through a given client.
   *
   * params.client     - MongoDB client connected to an instance.
   * params.database   - Database containing the collection.
   * params.collection - Collection to tail.
   */
  public MongoFollower(OpLogTailerParams params) {
    this.params = params;
  }

  private MongoCursor<Document> getNewCursor() throws Exception {
    logger.info("Getting oplog cursor for oplog time: " + params.globals.oplogTime.toString());

    client = new MongoClient(new MongoClientURI(params.getConnectionString()));
    // Get the oplog.
    MongoDatabase db = client.getDatabase("local");
    MongoCollection<Document> coll = db.getCollection(OPLOG_NAMESPACE).withReadPreference(ReadPreference.primaryPreferred());

    // NOTE:
    // Add to query { fromMigrate: {$exists: false}} to handle a sharded environment.
    // In a Primary/Secondary deployment tail the secondary only to deal with failover.
    // In a clustered deployment tail all oplogs and wait until there is a majority.
    // JGD: TODO - what if there is a change to RS membership, or an election chooses
    // a different primary?
    // 
    // JGD: In an actual deployment, each tailer process will be deployed to an instance
    // that contains only the databases its interested in. This means that though we
    // configure this process with a set of databases and only oplog records emitted
    // from them should be processed, we don't filter the query since it would be an
    // error to, e.g., configure a tailer process with "coredb" on a mongo instance that
    // contains the metrics dbs. As a safety check, we'll check the oplog namespace field
    // for each query result. If we tried to filter the query itself, we'd have to filter
    // using regex to check the namespace field, which is needlessly expensive.
    Document query = new Document()
        .append("ts", new BasicDBObject("$gte", params.globals.oplogTime));
        // .append("ns", new BasicDBObject("$eq", params.database + "." + params.collections.stream().findFirst().get()))
        // fromMigrate indicates the operation results from a shard rebalancing.
        // ** DO NOT UNCOMMENT IF NOT IN A SHARDED ENVIRONMENT!
        // .append("fromMigrate", new BasicDBObject("$exists", "false"));

    return coll.find(query)
    	// Is this needed for capped collections?
    	// JGD: yes, force collection scan
        .sort(NATURAL_SORT_FOR_COLLECTION_SCAN)
        .cursorType(CursorType.TailableAwait)
        .oplogReplay(true)
         //.noCursorTimeout(true)
        .iterator();
  }

  /**
   * Keep calling process with a new cursor. Add a delay in case process is
   * failing instantly.
   */
  @Override
  public void run() {
    try {
      if (params.doExport) {
        // Perform initial import
        logger.info("Starting initial export!");
        try (MongoClient exportClient = new MongoClient(new MongoClientURI(params.getConnectionString()))) {
          MongoCollection<Document> collection = exportClient.getDatabase(params.database.iterator().next())
                .getCollection(params.collections.stream().findFirst().get());
          InitialExporter exporter = new InitialExporter(params.oplogSink, Collections.singleton(collection));
          exporter.doExport();
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Exception during initial export!", e);
          System.exit(1);
        }
        logger.info("Initial import complete!");
      }

      // Tail the oplog
      while (params.globals.running.isRunning()) {
        logger.info("Starting oplog tail.");
        try {
          logger.info("Creating a new oplog cursor.");
          process(getNewCursor());
        } catch (Exception e) {
          logger.log(Level.SEVERE, "Error while processing oplog cursor.", e);
        }

        try {
          if (params.globals.running.isRunning()) {
            Thread.sleep(1000);
          }
        } catch (InterruptedException e) {
          logger.log(Level.SEVERE, "Oplog follower interrupted...", e);
        }
      }
    } finally {
      logger.severe("OpLogTailer thread is terminating.");
      client.close();
    }
  }

  /**
   * The main oplog processing loop.
   *
   * @param cursor
   *     the cursor to process.
   */
  private void process(MongoCursor<Document> cursor) throws Exception {
    while (params.globals.running.isRunning()) {
      while (cursor.hasNext() && params.globals.running.isRunning()) {
        Document d = cursor.next();
        boolean put = false;

        // If a document is taken, keep trying to add it to the oplogSink.
        while (!put && params.globals.running.isRunning()) {
         OplogEntry entry = OplogEntry.of(d);
      	  if (entry == null ) {
    	    logger.warning("Unable deserialize oplog entry; skipping");
      	    break;
      	  }
        	
          // TODO: make the check more natural
          if (shouldSkip(entry)) {
            logger.info("Skipping oplog entry type " + entry.getClass().getSimpleName());
            logger.finest("Skipped oplog entry document: " + entry.getRawEntry().toJson());
            break;
          }
        	
          if (!params.oplogSink.send(new Record(entry))) {
            logger.warning("Unable to send data to output oplogSink.");
          }
        }
      }

      // We need to wait for more data, the cursor has been exhausted.
      Thread.sleep(1000L);
    }
  }
  
  private boolean shouldSkip(OplogEntry entry) {
	  if (entry.shouldSkip()) {
		  logger.finest("Oplog entry skipped: " + entry.getClass().getSimpleName());
		  return true;
	  }
	  
	  String ns = entry.getNamespace();
	  if (!StringUtils.hasText(ns)) {
		  logger.finest("Oplog entry has no namespace; skipping");
		  return true;
	  }
	  
	  // There may be multiple '.' characters, but mongo doesn't allow database names to contain one.
	  int dotIdx = ns.indexOf('.');
	  if (dotIdx == -1) {
		  logger.warning("Skipping oplog entry with unrecognized namespace " + ns);
		  return true;
	  }
	  
	  String dbName = ns.substring(0, dotIdx);
	  if (!params.database.contains(dbName)) {
		  logger.warning("Skipping oplog entry with unprocessed database name " + dbName);
		  return true;
	  }
	  
	  return false;
  }
}

