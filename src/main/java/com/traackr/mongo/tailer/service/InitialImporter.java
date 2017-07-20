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
package com.traackr.mongo.tailer.service;

import com.traackr.mongo.tailer.model.Record;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerCursor;
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
        if (queue.offer(new Record(cur), 5, TimeUnit.SECONDS)) {
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

      ServerCursor serverCursor = cursor.getServerCursor();
      if (serverCursor != null) {
        // TODO: Persist cursor ID somewhere to allow restarts.
        this.cursorId = serverCursor.getId();
      }
    }
    else if (cursor == null && cursorId != 0) {
      // TODO: Lookup server cursor ID for resume.
      // Open existing cursor in case of restart??
      new Throwable().printStackTrace();
    }

    return cursor;
  }
}
