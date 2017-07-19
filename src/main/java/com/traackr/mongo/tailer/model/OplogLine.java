/**
 * OpLogLine.java - Traackr, Inc.
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

import java.util.Arrays;

/**
 * @author wwinder
 *         Created on: 5/27/16
 */
public class OplogLine {
  private final Document document;
  private final String id;
  private final BSONTimestamp timestamp;
  private final Operation operation;
  private final String namespace;
  private final Document updateSpec;
  private final Document query;

  public OplogLine(Document doc) {
    this.document = doc;

    BsonTimestamp ts = (BsonTimestamp) doc.get("ts");
    timestamp = new BSONTimestamp(ts.getTime(), ts.getInc());
    operation = Operation.getOpFor((String) doc.get("op"));
    namespace = (String) doc.get("ns");
    query = (Document) doc.get("o2");
    updateSpec = (Document) doc.get("o");

    switch(operation) {
      case DELETE:
      case INSERT:
        id = (String) updateSpec.get("_id");
        break;
      case UPDATE:
        id = (String) query.get("_id");
        break;
      default:
        id = "";
        break;
    }
  }

  public Document getDocument() {
    return document;
  }

  public String getId() {
    return id;
  }
  public BSONTimestamp getTimestamp() {
    return timestamp;
  }

  public Operation getOperation() {
    return operation;
  }

  public String getNamespace() {
    return namespace;
  }

  public Document getQuery() {
    return query;
  }

  public Document getUpdate() {
    return updateSpec;
  }

  public enum Operation {
    INSERT("i"),
    DELETE("d"),
    UPDATE("u"),
    COMMAND("c"),
    NOOP("n");

    final public String code;

    private Operation(String code) {
      this.code = code;
    }

    public static Operation getOpFor(String code) {
      return Arrays.stream(Operation.values())
          .filter(operator -> operator.code.equalsIgnoreCase(code))
          .findFirst()
          .orElse(null);
    }
  }
}



