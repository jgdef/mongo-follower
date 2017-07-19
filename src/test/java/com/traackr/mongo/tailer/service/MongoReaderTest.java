package com.traackr.mongo.tailer.service;

import static com.mongodb.client.model.Filters.gte;
import static com.traackr.mongo.tailer.service.helpers.EmbeddedMongo.createDocuments;
import static org.mockito.Mockito.times;

import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.OpLogTailerParams;
import com.traackr.mongo.tailer.model.Record;
import com.traackr.mongo.tailer.service.helpers.EmbeddedMongo;
import com.traackr.mongo.tailer.util.KillSwitch;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import de.flapdoodle.embed.mongo.distribution.Version;

import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wwinder on 6/17/16.
 */
// At this point I've been unable to get the `replicaSetStartMongo` method to properly start mongo
// with a replicaset and oplog.
public class MongoReaderTest {
  MongoConnector mc;
  GlobalParams globalParams;
  ArrayBlockingQueue<Record> queue;
  OpLogTailerParams params;
  MongoReader tailer;
  EmbeddedMongo em;
  String db = "test_db";
  String collection = "test_collection";

  @Before
  public void setUp() throws Exception {
    em = EmbeddedMongo.replicaSetStartMongo(Version.Main.V3_4);

    // Inject embedded mongo client.
    mc = Mockito.mock(MongoConnector.class);
    Mockito.doReturn(em.getClient()).when(mc).getClient();

    globalParams = new GlobalParams(
        true,
        new KillSwitch(),
        new BSONTimestamp(0, 0),
        "yyyy-MM-dd'T'HH:mm:ss",
        true);
    queue = Mockito.spy(new ArrayBlockingQueue<>(4000));

    params = OpLogTailerParams.with(
        globalParams,
        false,
        queue,
        mc,
        db,
        collection);

    tailer = new MongoReader(params);
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testConnection() throws Exception {
    // Start oplog tailer.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future future = executor.submit(tailer);

    // Add some documents to mongo / oplog.
    createDocuments(em, this.db, this.collection, 5, true);

    Thread.sleep(100);

    // Make sure the message were detected in the oplog.
    Mockito.verify(queue, times(5)).put(Mockito.any(Record.class));

    globalParams.running.kill();
    executor.shutdownNow();

    // Wait for graceful shutdown...
    int i = 0;
    while (i++ < 20 && !future.isDone()) {
      Thread.sleep(1000);
    }

    Assert.assertTrue(future.isDone());
  }

  /**
   * Make sure a new connection is created if the first one is closed.
   * @throws Exception
   */
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
        queue,
        mockConnector,
        this.db,
        this.collection);

    tailer = new MongoReader(params);

    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future future = executor.submit(tailer);

    // Get private field...
    Field field = MongoReader.class.getDeclaredField("client");
    field.setAccessible(true);

    MongoClient client = null;
    while (client == null) {
      client = (MongoClient) field.get(tailer);
    }

    client.close();

    // Wait for the tailer to request a new connection.
    int i = 0;
    while (client == one) {
      client = (MongoClient) field.get(tailer);
      Thread.sleep(1000);
    }

    Assert.assertEquals(client, two);
    Mockito.verify(mockConnector, times(2)).getClient();
  }
}
