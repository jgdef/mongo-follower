/**
 * OpLogTailerParams.java - Traackr, Inc.
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
package com.traackr.mongo.tailer.params;

import com.traackr.mongo.tailer.connection.MongoConnector;
import com.traackr.mongo.tailer.service.Record;

import java.util.concurrent.BlockingQueue;

import lombok.Value;

/**
 * @author wwinder
 *         Created on: 5/29/16
 */
@Value(staticConstructor="with")
public class OpLogTailerParams {
  public GlobalParams globals;
  public boolean doImport;
  public BlockingQueue<Record> queue;
  public MongoConnector connector;
  public String database;
  public String collection;
}

