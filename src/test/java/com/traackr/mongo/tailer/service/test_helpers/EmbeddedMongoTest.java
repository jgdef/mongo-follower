/**
 * EmbeddedMongoTest.java - Traackr, Inc.
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

package com.traackr.mongo.tailer.service.test_helpers;

import com.mongodb.BasicDBObject;

import de.flapdoodle.embed.mongo.distribution.Version;

import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author wwinder
 * Created on: 7/17/17
 */
public class EmbeddedMongoTest {

  EmbeddedMongo em;

  @Before
  public void setup() throws IOException {
    //em = replicaSetStartMongo(Version.Main.V3_3);
    em = EmbeddedMongo.simpleStartMongo(Version.Main.V3_4);
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
    em.col.findOneAndDelete(doc);

    // Before tests, nothing should be found.
    Assert.assertNull(em.col.find(doc).first());

    // Insert document, something should be found.
    em.col.insertOne(doc);
    Assert.assertNotNull(em.col.find(doc).first());

    // Removing document, nothing should be found.
    em.col.findOneAndDelete(doc);
    Assert.assertNull(em.col.find(doc).first());
  }
}
