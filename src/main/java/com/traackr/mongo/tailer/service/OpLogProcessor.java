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

import com.traackr.mongo.tailer.interfaces.MongoEventListener;
import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.OplogEntry;
import com.traackr.mongo.tailer.model.Record;

import org.bson.types.BSONTimestamp;

import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Process OplogEntry objects from a queue and send them to elasticsearch.
 *
 * @author wwinder
 * Created on: 5/25/16
 */
public class OpLogProcessor implements Runnable {
  private static final Logger logger = Logger.getLogger(OpLogProcessor.class.getName());

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
          logger.log(Level.SEVERE, "Exception while taking an op log document.", e);
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
      logger.log(Level.SEVERE, "A fatal exception occurred in the OpLogProcessor.", e);
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
        oplogEventListener.dispatch(entry);
      } catch (Exception e) {
        logger.log(Level.SEVERE, "Problem processing entry: " + entry, e);
        return false;
      }
    }
    return true;
  }
}
