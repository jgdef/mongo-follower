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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Tails the oplog dumping each record into a blocking queue for another thread
 * to process.
 *
 * @author wwinder
 * Created on: 5/25/16
 */
public class MongoFollower implements Runnable {
  private static final Logger logger = Logger.getLogger(MongoFollower.class.getName());

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
    MongoCollection<Document> coll = db.getCollection("oplog.rs");

    //Document query = new Document("{ indexedField: { $gt: <lastValue }}");
    // NOTE:
    // Add to query { fromMigrate: {$exists: false}} to handle a sharded environment.
    // In a Primary/Secondary deployment tail the secondary only to deal with failover.
    // In a clustered deployment tail all oplogs and wait until there is a majority.

    Document query = new Document()
        .append("ts", new BasicDBObject("$gte", params.globals.oplogTime))
        .append("ns", new BasicDBObject("$eq", params.database + "." + params.collection))
        // fromMigrate indicates the operation results from a shard rebalancing.
        //.append("fromMigrate", new BasicDBObject("$exists", "false"))
        ;

    // Is this needed for capped collections?
    BasicDBObject sort = new BasicDBObject("$natural", 1);

    return coll.find(query)
        .sort(sort)
        .cursorType(CursorType.TailableAwait)
        .oplogReplay(true)
        .iterator();
  }

  /**
   * Keep calling process with a new cursor. Add a delay in case process is
   * failing instantly.
   */
  public void run() {
    try {
      if (params.doExport) {
        // Perform initial import
        logger.info("Starting initial export!");
        try {
          MongoClient client = new MongoClient(new MongoClientURI(params.getConnectionString()));
          MongoCollection<Document> collection =
              client.getDatabase(params.database).getCollection(params.collection);
          InitialExporter exporter = new InitialExporter(params.queue, collection);
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

        // If a document is taken, keep trying to add it to the queue.
        while (!put && params.globals.running.isRunning()) {
          try {
            params.queue.put(new Record(OplogEntry.of(d)));
            put = true;
          } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Interrupted while saving data to output queue.", e);
          }
        }
      }

      // We need to wait for more data, the cursor has been exhausted.
      Thread.sleep(1000);
    }
  }


}

