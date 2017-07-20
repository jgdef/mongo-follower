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
package com.traackr.mongo.tailer.service;

import com.traackr.mongo.tailer.model.Delete;
import com.traackr.mongo.tailer.model.Insert;
import com.traackr.mongo.tailer.model.OplogEntry;
import com.traackr.mongo.tailer.model.Update;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.assertj.core.api.Assertions;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by wwinder on 6/24/16.
 */
public class OplogEntryTest {
  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  private static Document documentFromString(String s) throws IOException {
    return new Document((HashMap<String, Object>) new ObjectMapper().readValue(s, HashMap.class));
  }

  /**
   * @param timestamp
   * @param operation
   *     i/d/u
   * @param namespace
   *     "database.collection"
   * @param o
   *     for "d" and "i" operations this should be "{_id:'12345'}"
   * @param o2
   * @return
   * @throws IOException
   */
  public static Document createOplogDocument(
      int timestamp, String operation, String namespace, String o,
      String o2) throws IOException {
    Map<String, Object> docContent = new HashMap<>();
    docContent.put("v", 2);
    docContent.put("h", (long) 12345);
    docContent.put("ts", new BsonTimestamp(timestamp, 0));
    docContent.put("op", operation);
    docContent.put("ns", namespace);
    docContent.put("o", documentFromString(o));
    if (o2 != null) {
      docContent.put("o2", documentFromString(o2));
    }

    // DELETE / INSERT: id = (String) o.get("_id");
    // UPDATE: id = (String) o2.get("_id");

    return new Document(docContent);
  }

  static final String id = "50eaeb17aa413582b881b180f36504a3";
  static final String insertJson = "{\"_id\":\"" + id
                                   + "\",\"redirectId\":\"fef567fa6ae337d5a9628b5fe39b179b\"," +
                                   "\"url\":\"http://goo.gl/n64i7X\"}";
  static final String updateJson = "{\"$set\":{\"refCount\":\"1\",\"score\":\"116\",\"refs\":" +
                                   "{\"id\":\"" + id
                                   + "\",\"influencerId\":\"2e495e3b5ca640149aab020e3c33cd81\"," +
                                   "\"date\":\"Wed Apr 13 07:56:57 EDT 2016\","
                                   + "\"url\":\"http://twitter"
                                   + ".com/geekynerddad/statuses/720219220903071744\","
                                   +
                                   "\"reach\":\"70\",\"resonance\":\"46\","
                                   + "\"tkrType\":\"PERSON\"}}}";
  static final String idJson = "{\"_id\":\"" + id + "\"}";

  static public Document getOplogInsert() throws IOException {
    return createOplogDocument(1234, "i", "my.collection", insertJson, null);
  }

  static public Document getOplogWholesaleUpdate() throws IOException {
    return createOplogDocument(1234, "u", "my.collection", insertJson, idJson);
  }

  static public Document getOplogSetUpdate() throws IOException {
    return createOplogDocument(1234, "u", "my.collection", updateJson, idJson);
  }

  static public Document getOplogDelete() throws IOException {
    return createOplogDocument(1234, "d", "my.collection", idJson, null);
  }

  @Test
  public void oplogParsing() throws IOException {
    OplogEntry entry = OplogEntry.of(getOplogInsert());
    Assertions.assertThat(entry).isInstanceOf(Insert.class);
    Assertions.assertThat(entry.getId()).isEqualTo(OplogEntryTest.id);

    entry = OplogEntry.of(getOplogWholesaleUpdate());
    Assertions.assertThat(entry).isInstanceOf(Update.class);
    Assertions.assertThat(entry.getId()).isEqualTo(OplogEntryTest.id);

    entry = OplogEntry.of(getOplogSetUpdate());
    Assertions.assertThat(entry).isInstanceOf(Update.class);
    Assertions.assertThat(entry.getId()).isEqualTo(OplogEntryTest.id);

    entry = OplogEntry.of(getOplogDelete());
    Assertions.assertThat(entry).isInstanceOf(Delete.class);
    Assertions.assertThat(entry.getId()).isEqualTo(OplogEntryTest.id);
  }
}
