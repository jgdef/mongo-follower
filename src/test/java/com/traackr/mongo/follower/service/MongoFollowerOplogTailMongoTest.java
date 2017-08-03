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

import com.traackr.mongo.follower.model.GlobalParams;
import com.traackr.mongo.follower.model.OpLogTailerParams;
import com.traackr.mongo.follower.model.Record;
import com.traackr.mongo.follower.service.helpers.EmbeddedMongo;
import com.traackr.mongo.follower.util.KillSwitch;

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
public class MongoFollowerOplogTailMongoTest {
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
        new KillSwitch(),
        new BSONTimestamp((int) (timestamp.getTime() / 1000), 0),
        0,
        0,
        "yyyy-MM-dd'T'HH:mm:ss",
        true);
  }

  @Test
  public void noActivityOplogTailOnlyTest() throws InterruptedException {
    // These documents should be part of the oplog tailing, NOT the initial export.
    createDocuments(em, DATABASE, COLLECTION, 100, true);

    OpLogTailerParams params = OpLogTailerParams.with(
        globalsAtTime(new Date(0)), // start at the beginning of time
        false,
        spyQueue,
        em.getConnectionString(),
        DATABASE,
        COLLECTION);
    MongoFollower follower = new MongoFollower(params);

    // Start oplog tailing.
    Thread followerThread = new Thread(follower);
    followerThread.start();

    Thread.sleep(5000);

    Assert.assertEquals(100, spyQueue.size());
    spyQueue.stream().forEach(record -> Assert.assertNull(record.exportDocument));
  }

  @Test
  public void noActivityOplogExportTest() throws InterruptedException {
    // These documents should be part of the initial export.
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
    MongoFollower follower = new MongoFollower(params);

    // Start oplog tailing.
    Thread followerThread = new Thread(follower);
    followerThread.start();

    Thread.sleep(5000);

    Assert.assertEquals(100, spyQueue.size());
    spyQueue.stream().forEach(record -> Assert.assertNotNull(record.exportDocument));
  }

  @Test
  public void noActivityOplogExportWithOverlapTest() throws InterruptedException {
    OpLogTailerParams params = OpLogTailerParams.with(
        globalsAtTime(new Date()), // start at now
        true,
        spyQueue,
        em.getConnectionString(),
        DATABASE,
        COLLECTION);
    MongoFollower follower = new MongoFollower(params);

    // These documents will be part of the initial export and the oplog, technically thats ok.
    createDocuments(em, DATABASE, COLLECTION, 100, true);

    // Start oplog tailing.
    Thread followerThread = new Thread(follower);
    followerThread.start();

    Thread.sleep(5000);

    Assert.assertEquals(200, spyQueue.size());
    int i = 0;
    while (spyQueue.size() > 0) {
      if (i < 100) {
        Assert.assertNotNull(spyQueue.take().exportDocument);
      } else {
        Assert.assertNull(spyQueue.take().exportDocument);
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
    MongoFollower follower = new MongoFollower(params);

    // Start oplog tailing.
    Thread followerThread = new Thread(follower);
    followerThread.start();

    Thread.sleep(1000);

    // Insert documents AFTER the follower was started
    createDocuments(em, DATABASE, COLLECTION, 100, true);

    Thread.sleep(5000);

    Assert.assertEquals(100, spyQueue.size());
    spyQueue.stream().forEach(record -> Assert.assertNull(record.exportDocument));
  }
}
