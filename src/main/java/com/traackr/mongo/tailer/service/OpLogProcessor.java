/**
 * OpLogProcessor.java - Traackr, Inc.
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

import com.traackr.mongo.tailer.interfaces.MongoEventListener;
import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.OplogLine;
import com.traackr.mongo.tailer.model.Record;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Process OplogLine objects from a queue and send them to elasticsearch.
 *
 * @author wwinder
 *         Created on: 5/25/16
 */
public class OpLogProcessor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(OpLogProcessor.class);

  /**
   * Flush the update list every UPDATE_FLUH_INTERVAL_MS.
   */
  private final static int UPDATE_FLUSH_INTERVAL_MS = 5000;

  private final GlobalParams globals;
  private final BlockingQueue<Record> recordQueue;
  private final MongoEventListener oplogEventListener;
  private final List<OplogLine> updateQueue = new ArrayList<>();
  private long mark = System.currentTimeMillis();

  public OpLogProcessor(GlobalParams globals,
                        BlockingQueue<Record> recordQueue,
                        MongoEventListener oplogEventListener) {
    this.globals = globals;
    this.recordQueue = recordQueue;
    this.oplogEventListener = oplogEventListener;
  }

  /**
   * Main loop.
   */
  @Override
  public void run() {
    try {
      while (globals.running.isRunning()) {
        Record record = null;
        try {
          record = recordQueue.poll(timeUntilNextFlush(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          logger.error("Exception while taking an op log document.", e);
        }

        if (record != null) {

          // Initial import
          if (record.importLine != null) {
            oplogEventListener.importRecord(record.importLine);
          }

          // Oplog tail
          else if (record.oplogLine != null) {
            OplogLine doc = record.oplogLine;

            // Check for update flush.
            updateFlushCheck(doc);

            // Process the document, manage oplog timestamp on success.
            BSONTimestamp oplogTime = doc.getTimestamp();

            if (processDocument(doc)) {
              globals.oplogTime = doc.getTimestamp();
            }
          }
        }
      }

      logger.info("OpLogProcessor is shutting down.");
    } catch (Exception e) {
      logger.error("A fatal exception occurred in the OpLogProcessor.", e);
    }
  }

  /**
   * Returns the time in milliseconds until we need to flush the update queue.
   */
  private long timeUntilNextFlush() {
    return UPDATE_FLUSH_INTERVAL_MS - (System.currentTimeMillis() - mark) + 100;
  }

  /**
   * If the next operation isn't an UPDATE or the flush interval has elapsed,
   * we need to send the updates out.
   */
  private void updateFlushCheck(OplogLine doc) {
    long now = System.currentTimeMillis();

    if (updateQueue.size() == 0) {
      mark = now;
      return;
    }

    // Time has elapsed
    boolean doUpdate = (now - mark) > UPDATE_FLUSH_INTERVAL_MS;

    if (!doUpdate && doc != null) {
      // Next op isn't an update.
      doUpdate = (doc.getOperation() != OplogLine.Operation.UPDATE) || isWholesaleUpdate(doc);
    }

    if (doUpdate) {
      oplogEventListener.bulkUpdate(new ArrayList<>(updateQueue));
      updateQueue.clear();
      mark = System.currentTimeMillis();
    }
  }

  /**
   * Pass document to the indexing service. There is special handling for
   * updates, adjacent updates are batched together.
   * @param doc
   * @return
   */
  private boolean processDocument(OplogLine doc) {
    while (doc != null) {
      try {
        // Convert oplogline and send to indexer
        switch (doc.getOperation()) {
          case INSERT:
            oplogEventListener.insert(false, doc.getUpdate(), doc);
            break;
          case DELETE:
            oplogEventListener.delete(doc.getId(), doc);
            break;
          case UPDATE:
            if (isWholesaleUpdate(doc)) {
              oplogEventListener.insert(true, doc.getUpdate(), doc);
            } else {
              // Save for later...
              updateQueue.add(doc);
              //oplogEventListener.update(doc);
            }
            break;
          case COMMAND:
            oplogEventListener.command(doc.getDocument(), doc);
            break;
          case NOOP:
            break;
        }
        doc = null;
      } catch (Exception e) {
        logger.error("Problem indexing document: " + doc.getId(), e);
        return false;
      }
    }
    return true;
  }

  /**
   * Check if the oplog is an update updating the entire document.
   */
  private boolean isWholesaleUpdate(OplogLine doc) {
    return (doc.getOperation() == OplogLine.Operation.UPDATE) &&
           !doc.getUpdate().containsKey("$set") &&
           !doc.getUpdate().containsKey("$unset");
  }
}
