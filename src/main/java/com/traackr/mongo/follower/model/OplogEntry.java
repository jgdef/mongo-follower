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



