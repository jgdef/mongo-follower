/**
 * InitialImporterTest.java - Traackr, Inc.
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

package com.traackr.mongo.tailer.service;

import static com.traackr.mongo.tailer.service.helpers.EmbeddedMongo.createDocuments;
import static com.traackr.mongo.tailer.service.helpers.TestConstants.EMBEDDED_MONGO_VERSION;

import com.traackr.mongo.tailer.model.Record;
import com.traackr.mongo.tailer.service.helpers.EmbeddedMongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author wwinder
 * Created on: 7/19/17
 */
public class InitialImporterMongoTest {
  EmbeddedMongo em;

  @Before
  public void setup() throws IOException {
    em = EmbeddedMongo.replicaSetStartMongo(EMBEDDED_MONGO_VERSION);
  }

  @Test
  public void emptyInitialImport() {
    MongoClient client = em.getClient();
    MongoDatabase db = client.getDatabase("ii_test_db");
    MongoCollection col = db.getCollection("ii_test_collection");

    ArrayBlockingQueue<Record> queue = new ArrayBlockingQueue<>(2000);

    InitialImporter ii = new InitialImporter(queue, col);
    ii.doImport();

    Assert.assertEquals(0, queue.size());
  }

  @Test
  public void nonEmptyInitialImport() throws InterruptedException {
    MongoClient client = em.getClient();
    String database = "ii_test_db";
    String collection = "ii_test_collection";

    MongoDatabase db = client.getDatabase(database);
    MongoCollection col = db.getCollection(collection);

    // Add some documents to mongo / oplog for initial import.
    createDocuments(em, database, collection, 500, true);

    ArrayBlockingQueue<Record> queue = new ArrayBlockingQueue<Record>(2000);

    InitialImporter ii = new InitialImporter(queue, col);
    ii.doImport();

    Assert.assertEquals(500, queue.size());
  }
}
