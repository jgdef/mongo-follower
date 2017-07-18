/**
 * MongoConnector.java - Traackr, Inc.
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
package com.traackr.mongo.tailer.connection;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

/**
 * @author wwinder
 *         Created on: 5/25/16
 */
public class MongoConnector {
  private final String uri;
  public MongoConnector(String uri) {
    this.uri = uri;
  }

  public MongoClient getClient() throws Exception {
    return new MongoClient(new MongoClientURI(uri));
  }
}
