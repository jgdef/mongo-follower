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
package com.traackr.mongo.follower.service.helpers;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;

import de.flapdoodle.embed.mongo.distribution.Version;

import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;

/**
 * @author wwinder
 * Created on: 7/17/17
 */
public class EmbeddedMongoTest {

  EmbeddedMongo em;
  MongoCollection<Document> col;

  @Before
  public void setup() throws IOException {
    //em = replicaSetStartMongo(Version.Main.V3_3);
    em = EmbeddedMongo.simpleStartMongo(Version.Main.V3_4);

    MongoClient mongo = em.getClient();
    MongoDatabase db = mongo.getDatabase("test");
    db.createCollection("testCol", new CreateCollectionOptions());
    col = db.getCollection("testCol");
    col.insertOne(new Document("testDoc", new Date()));
  }

  @After
  public void teardown() {
    if (em != null) {
      em.stopMongo();
    }
  }

  @Test
  public void test() throws IOException {
    Document doc = new Document("name", "MongoDB")
        .append("type", "database")
        .append("count", 1)
        .append("info", new BasicDBObject("x", 203).append("y", 102));

    // Reset.
    col.findOneAndDelete(doc);

    // Before tests, nothing should be found.
    Assert.assertNull(col.find(doc).first());

    // Insert document, something should be found.
    col.insertOne(doc);
    Assert.assertNotNull(col.find(doc).first());

    // Removing document, nothing should be found.
    col.findOneAndDelete(doc);
    Assert.assertNull(col.find(doc).first());
  }
}
