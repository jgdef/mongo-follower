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

import com.traackr.mongo.follower.interfaces.MongoEventListener;
import com.traackr.mongo.follower.model.GlobalParams;
import com.traackr.mongo.follower.model.OplogEntry;
import com.traackr.mongo.follower.model.Record;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Process OplogEntry objects from a oplogSink and send them to the oplog sink.
 *
 * @author wwinder
 * Created on: 5/25/16
 */
public class OpLogProcessor implements Runnable {
  private static final Logger logger = Logger.getLogger(OpLogProcessor.class.getName());

  private final GlobalParams globals;
  private final OpLogSource oplogSource;
  private final MongoEventListener oplogEventListener;

  public OpLogProcessor(
      GlobalParams globals,
      OpLogSource oplogSource,
      MongoEventListener oplogEventListener) {
    this.globals = globals;
    this.oplogSource = oplogSource;
    this.oplogEventListener = oplogEventListener;
  }

  /**
   * Main loop.
   */
  @Override
  public void run() {
    try {
      while (globals.running.isRunning()) {
        Record record = (Record) oplogSource.take();
        if (record != null) {

          // Initial export
          if (record.exportDocument != null) {
            oplogEventListener.exportDocument(record.exportDocument);
          }

          // Oplog tail
          else if (record.oplogEntry != null) {
            OplogEntry entry = record.oplogEntry;

            // Process the document, manage oplog timestamp on success.
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
