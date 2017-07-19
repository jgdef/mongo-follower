package com.traackr.mongo.tailer.service;

import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.traackr.mongo.tailer.interfaces.MongoEventListener;
import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.OplogLine;
import com.traackr.mongo.tailer.model.Record;
import com.traackr.mongo.tailer.util.KillSwitch;

import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by wwinder on 6/23/16.
 */
public class OpLogProcessorTest {
  GlobalParams globalParams;
  ArrayBlockingQueue<Record> queue;
  MongoEventListener eventListener;

  @Captor
  private ArgumentCaptor<Collection<OplogLine>> bulkCaptor;

  @Before
  public void setUp() throws Exception {
    // Initialize "Captor" annotation.
    MockitoAnnotations.initMocks(this);

    globalParams = new GlobalParams(
        true,
        new KillSwitch(),
        new BSONTimestamp(0, 0),
        "yyyy-MM-dd'T'HH:mm:ss",
        true);
    queue = new ArrayBlockingQueue<>(4000);
    eventListener = Mockito.mock(MongoEventListener.class);
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testProcessQueue() throws Exception {
    boolean failed = false;

    queue.add(new Record(new OplogLine(OplogLineTest.getOplogInsert())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogWholesaleUpdate())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogDelete())));

    OpLogProcessor olp = new OpLogProcessor(globalParams, queue, eventListener);

    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future future = executor.submit(olp);

    int attempts = 20;
    while (attempts > 0 && queue.size() != 0) {
      attempts--;
      Thread.sleep(1000);
    }

    Assert.assertEquals(0, queue.size());

    globalParams.running.kill();
    executor.shutdownNow();
    attempts = 20;
    while (!future.isDone()) {
      attempts--;
      Thread.sleep(1000);
    }

    Assert.assertTrue(future.isDone());

    // Verify the data passed to the index service.
    ArgumentCaptor<Boolean> insertBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<Document> insertDoc = ArgumentCaptor.forClass(Document.class);
    ArgumentCaptor<OplogLine> insertOp = ArgumentCaptor.forClass(OplogLine.class);

    Mockito.verify(eventListener, times(2)).insert(
        insertBool.capture(), insertDoc.capture(), insertOp.capture());

    ArgumentCaptor<OplogLine> deleteOp = ArgumentCaptor.forClass(OplogLine.class);
    Mockito.verify(eventListener, times(1)).delete(Mockito.eq(OplogLineTest.id), deleteOp.capture());

    Assert.assertFalse(insertBool.getAllValues().get(0));
    Document d = insertDoc.getAllValues().get(0);
    Assert.assertTrue(d.containsKey("_id"));
    Assert.assertEquals(OplogLineTest.id, d.get("_id"));
    Assert.assertEquals(3, d.size());

    d = insertDoc.getAllValues().get(1);
    Assert.assertTrue(d.containsKey("_id"));
    Assert.assertEquals(OplogLineTest.id, d.get("_id"));
    Assert.assertEquals(3, d.size());
  }

  @Test
  public void updateFlushTest() throws Exception {
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogSetUpdate())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogSetUpdate())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogSetUpdate())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogSetUpdate())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogDelete())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogSetUpdate())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogSetUpdate())));
    queue.add(new Record(new OplogLine(OplogLineTest.getOplogDelete())));

    OpLogProcessor olp = new OpLogProcessor(globalParams, queue, eventListener);

    ExecutorService executor = Executors.newFixedThreadPool(1);
    Future future = executor.submit(olp);


    int attempts = 20;
    while (attempts > 0 && queue.size() > 0) {
      attempts--;
      Thread.sleep(1000);
    }
    Thread.sleep(1000);

    // Start graceful shutdown.
    globalParams.running.kill();
    executor.shutdownNow();

    attempts = 20;
    while (!future.isDone()) {
      attempts--;
      Thread.sleep(1000);
    }

    Assert.assertTrue(future.isDone());

    // Two updates. First should be 4, second should be 2.
    Mockito.verify(eventListener, times(2)).bulkUpdate(bulkCaptor.capture());
    Assert.assertEquals(4, bulkCaptor.getAllValues().get(0).size());
    Assert.assertEquals(2, bulkCaptor.getAllValues().get(1).size());

    Mockito.verify(eventListener, times(2)).delete(Mockito.eq(OplogLineTest.id), Mockito.any(OplogLine.class));
  }
}
