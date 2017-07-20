/**
 * OpLogEntry.java - Traackr, Inc.
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

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;

/**
 * @author wwinder
 * Created on: 5/27/16
 */
public abstract class OplogEntry {
  private final Document rawEntry;
  private final BSONTimestamp timestamp;
  private final String namespace;

  OplogEntry(Document doc) {
    this.rawEntry = doc;

    BsonTimestamp ts = doc.get("ts", BsonTimestamp.class);
    this.timestamp = new BSONTimestamp(ts.getTime(), ts.getInc());
    this.namespace = doc.getString("ns");
  }

  public static OplogEntry of(final Document doc) {
    final String operation = doc.getString("op");
    switch (operation) {
      case "i":
        return new Insert(doc);
      case "u":
        return new Update(doc);
      case "d":
        return new Delete(doc);
      case "c":
        return new Command(doc);
      case "n":
      case "db":
      default:
        return new Unhandled(doc);
    }
  }

  public Document getRawEntry() {
    return rawEntry;
  }

  public BSONTimestamp getTimestamp() {
    return timestamp;
  }

  public String getNamespace() {
    return namespace;
  }

  public abstract String getId();

  @Override
  public String toString() {
    return String.format("%s(id=%s)", this.getClass().getSimpleName(), this.getId());
  }
}



