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

import java.util.Map;
import java.util.logging.Logger;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;

/**
 * @author wwinder
 * Created on: 5/27/16
 */

/**
 * JGD: op log format (as of 3.4.9)"
 * {
 *   "ts": Timestamp,
 *   "t": NumberLong,
 *   "h": NumberLong,
 *   "v": int,
 *   "op": string,
 *   "ns": string,
 *   "o": Document
 *   "o2": Document
 * }
 *   
 *   "h" is a unique ID (hash), even for events with the same timestamp; 
 *       the values are never repeated in the same oplog
 *   "v" is the oplog format version, currently (v3.4.9) 2.
 *   "op" is a 1 or 2 character string describing the op log entry type: 'i' (insert), 'u' (update), 'd' (delete),
 *                                                                       'c' (command), 'db' (database-level commands), 
 *                                                                       'n' (no-op)
 *   "ns" is the 'namespace', typically "<database name>.<collection name>"
 *   "o"
 *   "o2" is only applicable to update oplog entries; it's the filter to identify the document(s) 
 *        that will be affected (by the data in the "o" field).
 */
public abstract class OplogEntry {
  private static final Logger logger = Logger.getLogger(OplogEntry.class.getName());
  private static final Integer OPLOG_VERSION = new Integer(2);
  public static final String OPLOG_FIELD_DOC = "o";
  public static final String OPLOG_FIELD_DOC_FILTER = "o2";  
  public static final String OPLOG_FIELD_TYPE = "op";
  public static final String OPLOG_INSERT = "i";
  public static final String OPLOG_UPDATE = "u";
  public static final String OPLOG_DELETE = "d";
  static final String OPLOG_COMMAND = "c";
  static final String OPLOG_DB_COMMAND = "db";
  static final String OPLOG_NOOP = "n";
  static final String OPLOG_FIELD_VERSION = "v";

  private final Document rawEntry;
  private final BSONTimestamp timestamp;
  private final String namespace;

  OplogEntry(Document doc) {
    rawEntry = doc;

    BsonTimestamp ts = doc.get("ts", BsonTimestamp.class);
    timestamp = new BSONTimestamp(ts.getTime(), ts.getInc());
    namespace = doc.getString("ns");
  }

  // CTOR used on the serialization side 
  OplogEntry(Map<String, Object> doc) {
	  rawEntry = new Document(doc);
	  BsonTimestamp ts = rawEntry.get("ts", BsonTimestamp.class);
	  timestamp = new BSONTimestamp(ts.getTime(), ts.getInc());
	  namespace = rawEntry.getString("ns");
  }
  
  public static OplogEntry of(final Document doc) {
	Integer version = doc.getInteger(OPLOG_FIELD_VERSION);
	if (version == null || !version.equals(OPLOG_VERSION)) {
		logger.severe("Unrecognized op log entry version " + version);
		return null;
	}

    final String operation = doc.getString(OPLOG_FIELD_TYPE);
    switch (operation) {
      case OPLOG_INSERT:
        return new Insert(doc);
      case OPLOG_UPDATE:
        return new Update(doc);
      case OPLOG_DELETE:
        return new Delete(doc);
      case OPLOG_COMMAND:
        return new Command(doc);
      case OPLOG_NOOP:
      case OPLOG_DB_COMMAND:
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

  public boolean shouldSkip() {
	  return false;
  }
  
  @Override
  public String toString() {
    return String.format("%s(id=%s)", this.getClass().getSimpleName(), this.getId());
  }
}