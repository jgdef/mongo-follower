package com.traackr.mongo.tailer.model;

import static com.traackr.mongo.tailer.model.Update.UpdateType.REPLACE;
import static com.traackr.mongo.tailer.model.Update.UpdateType.SET;
import static com.traackr.mongo.tailer.model.Update.UpdateType.UNSET;

import org.bson.Document;

import java.util.Map;

/**
 * @author Michael McLellan
 * Created on: 7/19/17
 */
public class Update extends OplogEntry {
  enum UpdateType {
    /**
     * Replace specified fields with new values
     */
    SET,

    /**
     * Remove specified fields from the document
     */
    UNSET,

    /**
     * Completely replace the existing document
     */
    REPLACE
  }

  private final UpdateType type;
  /**
   * Document of key:value pairs that were set, unset, or replaced into Mongo
   */
  private final Document document;
  /**
   * Mongo query that identifies the updated Document
   */
  private final Document query;

  Update(final Document doc) {
    super(doc);

    final Document updateSpec = doc.get("o", Document.class);
    if (updateSpec.containsKey("$set")) {
      this.type = SET;
      this.document = new Document(updateSpec.get("$set", Map.class));
    } else if (updateSpec.containsKey("$unset")) {
      this.type = UNSET;
      this.document = new Document(updateSpec.get("$unset", Map.class));
    } else {
      this.type = REPLACE;
      this.document = updateSpec;
    }

    this.query = doc.get("o2", Document.class);
  }

  public UpdateType getType() {
    return this.type;
  }

  public Document getQuery() {
    return this.query;
  }

  public Document getDocument() {
    return document;
  }

  @Override
  public String getId() {
    return this.query.getString("_id");
  }
}
