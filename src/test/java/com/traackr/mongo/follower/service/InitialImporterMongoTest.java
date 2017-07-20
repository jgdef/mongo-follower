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
package com.traackr.mongo.follower.service;

import static com.traackr.mongo.follower.service.helpers.EmbeddedMongo.createDocuments;
import static com.traackr.mongo.follower.service.helpers.TestConstants.EMBEDDED_MONGO_VERSION;

import com.traackr.mongo.follower.model.Record;
import com.traackr.mongo.follower.service.helpers.EmbeddedMongo;

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
