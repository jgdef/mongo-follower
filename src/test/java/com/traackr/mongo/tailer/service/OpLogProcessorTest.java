package com.traackr.mongo.tailer.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.traackr.mongo.tailer.interfaces.MongoEventListener;
import com.traackr.mongo.tailer.model.Delete;
import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.Insert;
import com.traackr.mongo.tailer.model.OplogEntry;
import com.traackr.mongo.tailer.model.Record;
import com.traackr.mongo.tailer.model.Update;
import com.traackr.mongo.tailer.util.KillSwitch;

import org.assertj.core.api.Assertions;
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
        0,
        0,
        "yyyy-MM-dd'T'HH:mm:ss",
        true);
    queue = new ArrayBlockingQueue<>(4000);
    eventListener = Mockito.mock(MongoEventListener.class);
    Mockito.doCallRealMethod().when(eventListener).dispatch(any(OplogEntry.class));
  }

  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testProcessQueue() throws Exception {
    queue.add(new Record(OplogEntry.of(OplogEntryTest.getOplogInsert())));
    queue.add(new Record(OplogEntry.of(OplogEntryTest.getOplogWholesaleUpdate())));
    queue.add(new Record(OplogEntry.of(OplogEntryTest.getOplogDelete())));

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

    // Verify the data passed to the listener
    ArgumentCaptor<Insert> insertOp = ArgumentCaptor.forClass(Insert.class);
    ArgumentCaptor<Update> updateOp = ArgumentCaptor.forClass(Update.class);

    Mockito.verify(eventListener, times(1)).insert(insertOp.capture());
    Mockito.verify(eventListener, times(1)).update(updateOp.capture());

    ArgumentCaptor<Delete> deleteOp = ArgumentCaptor.forClass(Delete.class);
    Mockito.verify(eventListener, times(1)).delete(deleteOp.capture());

    final Insert i = insertOp.getValue();
    Assertions.assertThat(i.getId()).isEqualTo(OplogEntryTest.id);
    Assertions.assertThat(i.getDocument()).hasSize(3);

    final Update u = updateOp.getValue();
    Assertions.assertThat(u.getId()).isEqualTo(OplogEntryTest.id);
    Assertions.assertThat(u.getDocument()).hasSize(3);

    final Delete d = deleteOp.getValue();
    Assertions.assertThat(d.getId()).isEqualTo(OplogEntryTest.id);
  }
}
