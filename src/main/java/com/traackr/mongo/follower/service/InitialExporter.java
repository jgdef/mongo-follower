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

import com.traackr.mongo.follower.model.Record;
import com.google.common.collect.ImmutableSet;
import com.mongodb.ServerCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import org.bson.Document;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * @author wwinder
 *         Created on: 12/14/16
 */
public class InitialExporter {
  private static final Logger logger = Logger.getLogger(OpLogProcessor.class.getName());
  private static final Document ALL_FILTER = Document.parse("{}");

  private final Set<MongoCollection<Document>> collections;
  private final OpLogSink sink;
  private long cursorId;    // not used yet
  private MongoCursor<Document> cursor;

  public InitialExporter(OpLogSink sink,
                         Set<MongoCollection<Document>> collections) {
    this.sink = sink;
    this.collections = ImmutableSet.copyOf(collections);
  }

  /**
   * Processes the cursor until nothing is left, sending all the documents to
   */
  public void doExport() {
    MongoCursor<Document> cursor = getCursor();
    Document cur = null;
    while(cur != null || cursor.hasNext()) {
      if (cur == null) {
        cur = cursor.next();
      }

      if (sink.send(new Record(cur), 5, TimeUnit.SECONDS)) {
        // Clear out current document.
        cur = null;
      } else {
        // Failed to put document into the oplogSink, don't clear 'cur' so another attempt is made.
        logger.warning("Failed to put next record in the oplogSink.");
      }
    }
  }

  /**
   * Get the MongoDB cursor.
   */
  private synchronized MongoCursor<Document> getCursor() {
    if (cursor == null) {
      FindIterable<Document> results = collections.stream().findFirst().get()
          .find(ALL_FILTER)
          .batchSize(1000)
          .noCursorTimeout(true)
          .sort(MongoFollower.NATURAL_SORT_FOR_COLLECTION_SCAN);
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
