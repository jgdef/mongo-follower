package com.traackr.mongo.tailer.service;

import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.traackr.mongo.tailer.interfaces.MongoEventListener;
import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.OplogEntry;
import com.traackr.mongo.tailer.model.Record;
import com.traackr.mongo.tailer.util.KillSwitch;

import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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

  @Before
  public void setUp() throws Exception {
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
    queue.add(new Record(new OplogEntry(OplogEntryTest.getOplogInsert())));
    queue.add(new Record(new OplogEntry(OplogEntryTest.getOplogWholesaleUpdate())));
    queue.add(new Record(new OplogEntry(OplogEntryTest.getOplogDelete())));

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
    ArgumentCaptor<Document> insertDoc = ArgumentCaptor.forClass(Document.class);
    ArgumentCaptor<OplogEntry> insertOp = ArgumentCaptor.forClass(OplogEntry.class);
    ArgumentCaptor<Boolean> updateBool = ArgumentCaptor.forClass(Boolean.class);
    ArgumentCaptor<OplogEntry> updateOp = ArgumentCaptor.forClass(OplogEntry.class);

    Mockito.verify(eventListener, times(1)).insert(insertDoc.capture(), insertOp.capture());
    Mockito.verify(eventListener, times(1)).update(updateBool.capture(), updateOp.capture());

    ArgumentCaptor<OplogEntry> deleteOp = ArgumentCaptor.forClass(OplogEntry.class);
    Mockito.verify(eventListener, times(1)).delete(Mockito.eq(OplogEntryTest.id), deleteOp.capture());

    Document d = insertDoc.getAllValues().get(0);
    Assert.assertTrue(d.containsKey("_id"));
    Assert.assertEquals(OplogEntryTest.id, d.get("_id"));
    Assert.assertEquals(3, d.size());

    d = updateOp.getAllValues().get(0).getUpdate();
    Assert.assertTrue(d.containsKey("_id"));
    Assert.assertEquals(OplogEntryTest.id, d.get("_id"));
    Assert.assertEquals(3, d.size());
  }
}
