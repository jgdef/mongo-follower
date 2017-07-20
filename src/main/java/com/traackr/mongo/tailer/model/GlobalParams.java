/**
 * GlobalParams.java - Traackr, Inc.
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

import com.traackr.mongo.tailer.util.KillSwitch;

import org.bson.types.BSONTimestamp;

import lombok.Data;

/**
 * @author wwinder
 *         Created on: 5/29/16
 */
@Data
public class GlobalParams {
  public GlobalParams(Boolean dryRun, KillSwitch running, BSONTimestamp oplogTime,
                      int oplogDelay, int oplogInterval, String dateFormat, Boolean longToString) {
    this.dryRun = dryRun;
    this.running = running;
    this.oplogTime = oplogTime;
    this.oplogDelay = oplogDelay;
    this.oplogInterval = oplogInterval;
    this.dateFormat = dateFormat;
    this.longToString = longToString;
  }

  public Boolean dryRun;
  public KillSwitch running;
  public BSONTimestamp oplogTime;
  public Integer oplogDelay;
  public Integer oplogInterval;
  public String dateFormat;
  public Boolean longToString;
}
