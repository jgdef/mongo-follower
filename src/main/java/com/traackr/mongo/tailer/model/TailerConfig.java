/**
 * TailerConfig.java - Traackr, Inc.
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

import com.traackr.mongo.tailer.interfaces.MongoEventListener;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * @author wwinder
 *         Created on: 7/18/17
 */
@Data
@Builder
public class TailerConfig {
  /**
   * How many minutes to lag behind the oplog to allow for overlapping events in case of restart
   * instead of lost events.
   */
  @NonNull
  @Builder.Default
  Integer oplogDelayMinutes = 10;

  /**
   * How frequently to update the oplog.
   */
  @NonNull
  @Builder.Default
  Integer oplogUpdateIntervalMinutes = 10;

  /**
   * The thread-safe queue to use.
   */
  @NonNull
  @Builder.Default
  BlockingQueue<Record> queue = new ArrayBlockingQueue<>(4000);

  /**
   * The callback handler.
   */
  @NonNull
  MongoEventListener listener;

  /**
   * Location of oplog file.
   */
  @NonNull
  String oplogFile;

  /**
   * ???
   */
  @NonNull
  @Builder.Default
  Boolean dryRun = true;

  /**
   * Mongo connection string.
   */
  @NonNull
  String mongoConnectionString;

  /**
   * Which database to tail.
   */
  @NonNull
  String mongoDatabase;

  /**
   * Which collection to tail.
   */
  @NonNull
  String mongoCollection;

  /**
   * Whether to do an initial import on the collection before oplog tailing.
   * TODO: Default value.
   */
  @NonNull
  @Builder.Default
  Boolean initialImport = false;
}
