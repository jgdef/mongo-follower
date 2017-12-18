/**
 * MIT License
 *
 * Copyright (c) 2017 Traackr, Inc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *   SOFTWARE.
 */
package com.traackr.mongo.follower.model;

import static com.traackr.mongo.follower.model.Update.UpdateType.REPLACE;
import static com.traackr.mongo.follower.model.Update.UpdateType.SET;
import static com.traackr.mongo.follower.model.Update.UpdateType.UNSET;

import org.bson.Document;

import java.util.Map;

/**
 * @author Michael McLellan
 * Created on: 7/19/17
 */
public class Update extends OplogEntry {
  public enum UpdateType {
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

  @SuppressWarnings("unchecked")
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

    this.query = doc.get(OPLOG_FIELD_DOC_FILTER, Document.class);
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
