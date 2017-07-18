/**
 * InitialImporter.java - Traackr, Inc.
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

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author wwinder
 *         Created on: 12/14/16
 */
public class InitialImporter {
  private static final Logger logger = LoggerFactory.getLogger(OpLogProcessor.class);

  private final MongoCollection<Document> collection;
  private final BlockingQueue<Record> queue;
  private long cursorId;
  private MongoCursor<Document> cursor;

  public InitialImporter(BlockingQueue<Record> queue,
                         MongoCollection<Document> collection) {
    this.queue = queue;
    this.collection = collection;
  }

  /**
   * Processes the cursor until nothing is left, sending all the documents to
   */
  public void doImport() {
    MongoCursor<Document> cursor = getCursor();
    Document cur = null;
    while(cur != null || cursor.hasNext()) {
      if (cur == null) {
        cur = cursor.next();
      }

      try {
        if (queue.offer(new Record(cursor.next()), 5, TimeUnit.SECONDS)) {
          // Clear out current document.
          cur = null;
        } else {
          // Failed to put document into the queue, don't clear 'cur' so another attempt is made.
          logger.warn("Failed to put next record in the queue.");
        }
      } catch (InterruptedException e) {
        // This is fine, keep trying.
        logger.warn("Queue offer interrupted.", e);
      }
    }
  }

  /**
   * Get the MongoDB cursor.
   */
  private MongoCursor<Document> getCursor() {
    if (cursor == null) {
      FindIterable<Document> results = collection
          .find(Document.parse("{}"))
          .sort(new BasicDBObject("$natural", 1))
          .noCursorTimeout(true);
      cursor = results.iterator();

      // TODO: Persist cursor ID somewhere to allow restarts.
      this.cursorId = cursor.getServerCursor().getId();
    }
    else if (cursor == null && cursorId != 0) {
      // TODO: Lookup cursor ID for resume.
      // Open existing cursor in case of restart??
      new Throwable().printStackTrace();
    }

    return cursor;
  }
}
