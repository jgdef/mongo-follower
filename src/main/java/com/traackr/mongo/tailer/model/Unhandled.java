package com.traackr.mongo.tailer.model;

import org.bson.Document;

/**
 * @author Michael McLellan
 * Created on: 7/19/17
 */
public class Unhandled extends OplogEntry {
  Unhandled(Document doc) {
    super(doc);
  }

  @Override
  public String getId() {
    return null;
  }
}
