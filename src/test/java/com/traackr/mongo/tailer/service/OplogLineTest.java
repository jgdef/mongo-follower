package com.traackr.mongo.tailer.service;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by wwinder on 6/24/16.
 */
public class OplogLineTest {
  @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }

  private static Document documentFromString(String s) throws IOException {
    return new Document((HashMap<String,Object>) new ObjectMapper().readValue(s, HashMap.class));
  }

  /**
   * @param time
   * @param operation i/d/u
   * @param namespace "database.collection"
   * @param o for "d" and "i" operations this should be "{_id:'12345'}"
   * @param o2
   * @return
   * @throws IOException
   */
  public static Document createOplogDocument(int timestamp, String operation, String namespace, String o,
                                 String o2) throws IOException {
    Map<String, Object> docContent = new HashMap<>();
    docContent.put("v", 2);
    docContent.put("h",(long)12345);
    docContent.put("ts", new BsonTimestamp(timestamp,0));
    docContent.put("op", operation);
    docContent.put("ns", namespace);
    docContent.put("o", documentFromString(o));
    if (o2 != null)
      docContent.put("o2", documentFromString(o2));

    // DELETE / INSERT: id = (String) o.get("_id");
    // UPDATE: id = (String) o2.get("_id");

    return new Document(docContent);
  }

  static final String id = "50eaeb17aa413582b881b180f36504a3";
  static final String insertJson = "{\"_id\":\""+id+"\",\"redirectId\":\"fef567fa6ae337d5a9628b5fe39b179b\"," +
      "\"url\":\"http://goo.gl/n64i7X\"}";
  static final String updateJson = "{\"$set\":{\"refCount\":\"1\",\"score\":\"116\",\"refs\":" +
      "{\"id\":\""+id+"\",\"influencerId\":\"2e495e3b5ca640149aab020e3c33cd81\"," +
      "\"date\":\"Wed Apr 13 07:56:57 EDT 2016\",\"url\":\"http://twitter.com/geekynerddad/statuses/720219220903071744\"," +
      "\"reach\":\"70\",\"resonance\":\"46\",\"tkrType\":\"PERSON\"}}}";
  static final String idJson = "{\"_id\":\""+id+"\"}";

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

  @org.junit.Test
  public void oplogParsing() throws IOException {
    OplogLine line = new OplogLine(getOplogInsert());
    assertEquals(OplogLine.Operation.INSERT, line.getOperation());
    assertEquals("50eaeb17aa413582b881b180f36504a3", line.getId());

    line = new OplogLine(getOplogWholesaleUpdate());
    assertEquals(OplogLine.Operation.UPDATE, line.getOperation());
    assertEquals("50eaeb17aa413582b881b180f36504a3", line.getId());

    line = new OplogLine(getOplogSetUpdate());
    assertEquals(OplogLine.Operation.UPDATE, line.getOperation());
    assertEquals("50eaeb17aa413582b881b180f36504a3", line.getId());

    line = new OplogLine(getOplogDelete());
    assertEquals(OplogLine.Operation.DELETE, line.getOperation());
    assertEquals("50eaeb17aa413582b881b180f36504a3", line.getId());
  }
}
