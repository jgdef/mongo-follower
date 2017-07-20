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
import com.traackr.mongo.tailer.model.OplogEntry;
import com.traackr.mongo.tailer.model.Record;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Process OplogEntry objects from a queue and send them to elasticsearch.
 *
 * @author wwinder
 * Created on: 5/25/16
 */
public class OpLogProcessor implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(OpLogProcessor.class);

  private final GlobalParams globals;
  private final BlockingQueue<Record> recordQueue;
  private final MongoEventListener oplogEventListener;

  public OpLogProcessor(
      GlobalParams globals,
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
          record = recordQueue.take();
        } catch (InterruptedException e) {
          logger.error("Exception while taking an op log document.", e);
        }

        if (record != null) {

          // Initial import
          if (record.importDocument != null) {
            oplogEventListener.importDocument(record.importDocument);
          }

          // Oplog tail
          else if (record.oplogEntry != null) {
            OplogEntry entry = record.oplogEntry;

            // Process the document, manage oplog timestamp on success.
            BSONTimestamp oplogTime = entry.getTimestamp();

            if (processEntry(entry)) {
              globals.oplogTime = entry.getTimestamp();
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
   * Pass oplog entry to the listener
   *
   * @param entry
   * @return true if no Exception
   */
  private boolean processEntry(OplogEntry entry) {
    if (entry != null) {
      try {
        oplogEventListener.process(entry);
      } catch (Exception e) {
        logger.error("Problem processing entry: {}", entry, e);
        return false;
      }
    }
    return true;
  }
}
