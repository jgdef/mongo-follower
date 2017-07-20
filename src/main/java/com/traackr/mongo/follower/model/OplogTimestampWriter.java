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
package com.traackr.mongo.follower.model;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.traackr.mongo.follower.util.OplogTimestampHelper;

import org.bson.types.BSONTimestamp;

/**
 * @author wwinder
 * Created on: 7/20/17
 */
public class OplogTimestampWriter implements Runnable {
  private final GlobalParams globals;

  public OplogTimestampWriter(GlobalParams globals) {
    this.globals = globals;
  }

  @Override
  public void run() {
    int delaySeconds = (int) MINUTES.toSeconds(globals.oplogDelay);
    long intervalMs = MINUTES.toMillis(globals.oplogInterval);
    while (!Thread.interrupted()) {
      try {
        BSONTimestamp ts = new BSONTimestamp(this.globals.oplogTime.getTime() - delaySeconds, 0);
        OplogTimestampHelper.saveOplogTimestamp(ts);
      } catch (Exception e) {
        e.printStackTrace();
      }

      try {
        Thread.sleep(intervalMs);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
