/**
 * MongoReaderOplogTailMongoTest.java - Traackr, Inc.
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

import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.OpLogTailerParams;
import com.traackr.mongo.tailer.model.Record;
import com.traackr.mongo.tailer.service.helpers.EmbeddedMongo;
import com.traackr.mongo.tailer.util.KillSwitch;

import org.bson.types.BSONTimestamp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author wwinder
 * Created on: 7/19/17
 */
public class MongoReaderOplogTailMongoTest {
  EmbeddedMongo em;
  BlockingQueue<Record> spyQueue = Mockito.spy(new ArrayBlockingQueue<>(4000));
  static final String DATABASE = "mrot_test_db";
  static final String COLLECTION = "mrot_test_collection";


  @Before
  public void setup() throws Exception {
    em = EmbeddedMongo.replicaSetStartMongo(EMBEDDED_MONGO_VERSION);
  }

  private static GlobalParams globalsAtTime(Date timestamp) {
    return new GlobalParams(
        true,
        new KillSwitch(),
        new BSONTimestamp((int) (timestamp.getTime() / 1000), 0),
        0,
        0,
        "yyyy-MM-dd'T'HH:mm:ss",
        true);
  }

  @Test
  public void noActivityOplogTailOnlyTest() throws InterruptedException {
    // These documents should be part of the oplog tailing, NOT the initial import.
    createDocuments(em, DATABASE, COLLECTION, 100, true);

    OpLogTailerParams params = OpLogTailerParams.with(
        globalsAtTime(new Date(0)), // start at the beginning of time
        false,
        spyQueue,
        em.getConnectionString(),
        DATABASE,
        COLLECTION);
    MongoReader tailer = new MongoReader(params);

    // Start oplog tailing.
    Thread tailerThread = new Thread(tailer);
    tailerThread.start();

    Thread.sleep(5000);

    Assert.assertEquals(100, spyQueue.size());
    spyQueue.stream().forEach(record -> Assert.assertNull(record.importDocument));
  }

  @Test
  public void noActivityOplogImportTest() throws InterruptedException {
    // These documents should be part of the initial import.
    createDocuments(em, DATABASE, COLLECTION, 100, true);

    // Give mongo a chance to get them into the oplog
    Thread.sleep(1000);

    OpLogTailerParams params = OpLogTailerParams.with(
        globalsAtTime(new Date()), // start at now
        true,
        spyQueue,
        em.getConnectionString(),
        DATABASE,
        COLLECTION);
    MongoReader tailer = new MongoReader(params);

    // Start oplog tailing.
    Thread tailerThread = new Thread(tailer);
    tailerThread.start();

    Thread.sleep(5000);

    Assert.assertEquals(100, spyQueue.size());
    spyQueue.stream().forEach(record -> Assert.assertNotNull(record.importDocument));
  }

  @Test
  public void noActivityOplogImportWithOverlapTest() throws InterruptedException {
    OpLogTailerParams params = OpLogTailerParams.with(
        globalsAtTime(new Date()), // start at now
        true,
        spyQueue,
        em.getConnectionString(),
        DATABASE,
        COLLECTION);
    MongoReader tailer = new MongoReader(params);

    // These documents will be part of the initial import and the oplog, technically thats ok.
    createDocuments(em, DATABASE, COLLECTION, 100, true);

    // Start oplog tailing.
    Thread tailerThread = new Thread(tailer);
    tailerThread.start();

    Thread.sleep(5000);

    Assert.assertEquals(200, spyQueue.size());
    int i = 0;
    while (spyQueue.size() > 0) {
      if (i < 100) {
        Assert.assertNotNull(spyQueue.take().importDocument);
      } else {
        Assert.assertNull(spyQueue.take().importDocument);
      }
      i++;
    }
    Assert.assertEquals(200, i);
  }

  @Test
  public void activityTest() throws InterruptedException {
    OpLogTailerParams params = OpLogTailerParams.with(
        globalsAtTime(new Date(0)), // start at the beginning of time
        false,
        spyQueue,
        em.getConnectionString(),
        DATABASE,
        COLLECTION);
    MongoReader tailer = new MongoReader(params);

    // Start oplog tailing.
    Thread tailerThread = new Thread(tailer);
    tailerThread.start();

    Thread.sleep(1000);

    // Insert documents AFTER the tailer was started
    createDocuments(em, DATABASE, COLLECTION, 100, true);

    Thread.sleep(5000);

    Assert.assertEquals(100, spyQueue.size());
    spyQueue.stream().forEach(record -> Assert.assertNull(record.importDocument));
  }
}
