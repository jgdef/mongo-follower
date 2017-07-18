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

import com.sun.istack.internal.NotNull;

import lombok.Builder;
import lombok.Data;

/**
 * @author wwinder
 *         Created on: 7/18/17
 */
@Data
@Builder
public class TailerConfig {
  /**
   * The callback handler.
   */
  @NotNull
  MongoEventListener listener;

  /**
   * Location of oplog file.
   */
  @NotNull
  String oplogFile;

  /**
   * ???
   */
  @NotNull
  Boolean dryRun;

  /**
   * Mongo connection string.
   */
  @NotNull
  String mongoConnectionString;

  /**
   * Which database to tail.
   */
  @NotNull
  String mongoDatabase;

  /**
   * Which collection to tail.
   */
  @NotNull
  String mongoCollection;

  /**
   * Whether to do an initial import on the collection before oplog tailing.
   * TODO: Default value.
   */
  @NotNull
  Boolean initialImport;

  /**
   * Number of messages to hold in memory before the oplog tailer begins to block.
   * TODO: Default value.
   */
  @NotNull
  Integer queueSize;
}
