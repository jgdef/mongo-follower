/**
 * OpLogTail.java - Traackr, Inc.
 *
 * This document set is the property of Traackr, Inc., a Massachusetts
 * Corporation, and contains confidential and trade secret information. It
 * cannot be transferred from the custody or control of Traackr except as
 * authorized in writing by an officer of Traackr. Neither this item nor the
 * information it contains can be used, transferred, reproduced, published,
 * or disclosed, in whole or in part, directly or indirectly, except as
 * expressly authorized by an officer of Traackr, pursuant to written
 * agreement.
 *
 * Copyright 2012-2015 Traackr, Inc. All Rights Reserved.
 */

package com.traackr.mongo.tailer.service;

import com.traackr.mongo.tailer.model.OpLogTailerParams;
import com.traackr.mongo.tailer.model.OplogEntry;
import com.traackr.mongo.tailer.model.Record;

import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tails the oplog dumping each record into a blocking queue for another thread
 * to process.
 *
 * @author wwinder
 * Created on: 5/25/16
 */
public class MongoReader implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(MongoReader.class);

  final private OpLogTailerParams params;
  private MongoClient client;

  /**
   * Create an oplog iterator through a given client.
   *
   * params.client     - MongoDB client connected to an instance.
   * params.database   - Database containing the collection.
   * params.collection - Collection to tail.
   */
  public MongoReader(OpLogTailerParams params) {
    this.params = params;
  }

  private MongoCursor<Document> getNewCursor() throws Exception {
    logger.info("Getting oplog cursor for oplog time: " + params.globals.oplogTime.toString());

    client = params.connector.getClient();
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
      if (params.doImport) {
        // Perform initial import
        logger.info("Starting initial import!");
        try {
          MongoClient client = params.connector.getClient();
          MongoCollection<Document> collection =
              client.getDatabase(params.database).getCollection(params.collection);
          InitialImporter importer = new InitialImporter(params.queue, collection);
          importer.doImport();
        } catch (Exception e) {
          logger.error("Exception during initial import!", e);
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
          logger.error("Error while processing oplog cursor.", e);
        }

        try {
          if (params.globals.running.isRunning()) {
            Thread.sleep(1000);
          }
        } catch (InterruptedException e) {
          logger.error("Oplog tailer interrupted...", e);
        }
      }
    } finally {
      logger.error("OpLogTailer thread is terminating.");
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
            logger.error("Interrupted while saving data to output queue.", e);
          }
        }
      }

      // We need to wait for more data, the cursor has been exhausted.
      Thread.sleep(1000);
    }
  }


}

