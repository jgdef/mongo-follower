/**
 * OplogTimestampWriter.java - Traackr, Inc.
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

package com.traackr.mongo.tailer.model;

import static java.util.concurrent.TimeUnit.MINUTES;

import com.traackr.mongo.tailer.util.OplogTimestampHelper;

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
