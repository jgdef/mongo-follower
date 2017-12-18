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
import static org.mockito.Mockito.times;

import com.traackr.mongo.follower.model.GlobalParams;
import com.traackr.mongo.follower.model.OpLogTailerParams;
import com.traackr.mongo.follower.model.Record;
import com.traackr.mongo.follower.service.helpers.EmbeddedMongo;
import com.traackr.mongo.follower.util.KillSwitch;

import de.flapdoodle.embed.mongo.distribution.Version;

import org.bson.types.BSONTimestamp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wwinder on 6/17/16.
 */
public class MongoFollowerTest {
/*	
  GlobalParams globalParams;
  ArrayBlockingQueue<Record> oplogSink;
  OpLogTailerParams params;
  MongoFollower follower;
  EmbeddedMongo em;
  String db = "test_db";
  String collection = "test_collection";

  @Before
  public void setUp() throws Exception {
    em = EmbeddedMongo.replicaSetStartMongo(Version.Main.V3_4);

    globalParams = new GlobalParams(
        new KillSwitch(),
        new BSONTimestamp(0, 0),
        0,
        0,
        "yyyy-MM-dd'T'HH:mm:ss",
        true);
    oplogSink = Mockito.spy(new ArrayBlockingQueue<>(4000));

    params = OpLogTailerParams.with(
        globalParams,
        false,
        oplogSink,
        em.getConnectionString(),
        db,
        Collections.singleton(collection));

    follower = new MongoFollower(params);
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testConnection() throws Exception {
    // Start oplog follower.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future future = executor.submit(follower);

    // Add some documents to mongo / oplog.
    createDocuments(em, this.db, this.collection, 5, true);

    Thread.sleep(100);

    // Make sure the message were detected in the oplog.
    Mockito.verify(oplogSink, times(5)).put(Mockito.any(Record.class));

    globalParams.running.kill();
    executor.shutdownNow();

    // Wait for graceful shutdown...
    int i = 0;
    while (i++ < 20 && !future.isDone()) {
      Thread.sleep(1000);
    }

    Assert.assertTrue(future.isDone());
  }
*/
  /**
   * Make sure a new connection is created if the first one is closed.
   * @throws Exception
   */
  // TODO: Can this test still be done without the MongoConnector object?
  /*
  @Test
  public void testMongoReaderReconnect() throws Exception {
    MongoConnector mockConnector = Mockito.mock(MongoConnector.class);

    MongoClient one = em.getClient();
    MongoClient two = em.getClient();
    Mockito.doReturn(one).doReturn(two)
        .when(mockConnector).getClient();

    params = OpLogTailerParams.with(
        globalParams,
        false,
        oplogSink,
        em.getConnectionString(),
        this.db,
        this.collection);

    follower = new MongoReader(params);

    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future future = executor.submit(follower);

    // Get private field...
    Field field = MongoReader.class.getDeclaredField("client");
    field.setAccessible(true);

    MongoClient client = null;
    while (client == null) {
      client = (MongoClient) field.get(follower);
    }

    client.close();

    // Wait for the follower to request a new connection.
    int i = 0;
    while (client == one) {
      client = (MongoClient) field.get(follower);
      Thread.sleep(1000);
    }

    Assert.assertEquals(client, two);
    Mockito.verify(mockConnector, times(2)).getClient();
  }
  */
}
