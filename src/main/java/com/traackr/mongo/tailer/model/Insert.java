package com.traackr.mongo.tailer.model;

import org.bson.Document;

/**
 * @author Michael McLellan
 * Created on: 7/19/17
 */
public class Insert extends OplogEntry {
  /**
   * Full Document inserted into Mongo
   */
  private final Document document;

  Insert(Document doc) {
    super(doc);

    this.document = doc.get("o", Document.class);
  }

  public Document getDocument() {
    return this.document;
  }

  @Override
  public String getId() {
    return this.document.getString("_id");
  }
}
