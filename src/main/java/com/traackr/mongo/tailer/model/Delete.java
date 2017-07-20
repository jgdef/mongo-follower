package com.traackr.mongo.tailer.model;

import org.bson.Document;

/**
 * @author Michael McLellan
 * Created on: 7/19/17
 */
public class Delete extends OplogEntry {
  /**
   * Mongo query that identifies the deleted Document
   */
  private final Document query;

  Delete(Document doc) {
    super(doc);

    this.query = doc.get("o", Document.class);
  }

  public Document getQuery() {
    return query;
  }

  @Override
  public String getId() {
    return query.getString("_id");
  }
}
