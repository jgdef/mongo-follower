/**
 * Runner.java - Traackr, Inc.
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

import static com.traackr.mongo.tailer.util.OplogTimestampHelper.getOplogTimestamp;

import com.traackr.mongo.tailer.exceptions.FailedToStartException;
import com.traackr.mongo.tailer.interfaces.MongoEventListener;
import com.traackr.mongo.tailer.model.GlobalParams;
import com.traackr.mongo.tailer.model.OpLogTailerParams;
import com.traackr.mongo.tailer.model.Record;
import com.traackr.mongo.tailer.model.TailerConfig;
import com.traackr.mongo.tailer.util.KillSwitch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wwinder
 *         Created on: 7/18/17
 */
public class Runner {
  public static void run(TailerConfig config) throws FailedToStartException {
    ArrayBlockingQueue<Record> queue = new ArrayBlockingQueue<>(config.getQueueSize());

    // Initialize oplog dir.
    String oplog = config.getOplogFile();
    Path parentDirectory = Paths.get(oplog).getParent();
    try {
      Files.createDirectories(parentDirectory);
    }
    catch (IOException ioe) {
      throw new FailedToStartException("Failed to find or create oplog file.", ioe);
    }

    // Global properties.
    GlobalParams globalParams = new GlobalParams(
        config.getDryRun(),
        new KillSwitch(),
        getOplogTimestamp(oplog),
        null,
        false
    );

    // Mongo properties
    String mongoConnectionString = config.getMongoConnectionString();
    OpLogTailerParams mongoParams = null;
    try {
      MongoConnector mc = new MongoConnector(mongoConnectionString);
      mongoParams = OpLogTailerParams.with(
          globalParams,
          config.getInitialImport(),
          queue,
          mc,
          config.getMongoDatabase(),
          config.getMongoCollection());
    } catch (Exception e) {
      throw new FailedToStartException("Problem initializing mongo properties.", e);
    }

    // Initialize OpLogTail
    MongoReader oplogTailer = new MongoReader(mongoParams);

    // Initialize OpLogProcessor
    OpLogProcessor oplogProcessor = new OpLogProcessor(globalParams, queue, config.getListener());

    ///////////////////
    // Start threads //
    ///////////////////
    launchThreads(queue, oplogTailer, oplogProcessor);

  }

  private static void launchThreads(
                             final BlockingQueue<Record> queue,
                             MongoReader oplogTailer,
                             OpLogProcessor oplogProcessor) {
    final ThreadPoolExecutor pool = new ThreadPoolExecutor(3, 3,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());

    pool.submit(oplogTailer);
    pool.submit(oplogProcessor);

    // This seems weird.
    pool.submit(() -> {
      while (!Thread.interrupted() && pool.getActiveCount() != 0) {
        //logger.info("OplogQueue size: " + queue.size());

        if (pool.getActiveCount() == 0) {
          //logger.error("The threads are gone!!");
          System.exit(-1);
        } else if (pool.getActiveCount() != 2) {
          //logger.error("Only one processor thread is running!!");
        }
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          //logger.error("Sleep threw an exception.", e);
          Thread.interrupted();
        }
      }
    });
  }

  /**
   * Load settings from property file:
   * dry-run
   * oplog-file
   * initial-import
   * mongo.connection-string
   * mongo.database
   * mongo.collection
   */
  public static void run(Properties properties, MongoEventListener listener) throws FailedToStartException {
    TailerConfig config = TailerConfig.builder()
        .listener(listener)
        .dryRun(Boolean.valueOf(properties.getProperty("dry-run")))
        .oplogFile(properties.getProperty("oplog-file"))
        .initialImport(Boolean.valueOf(properties.getProperty("initial-import")))
        .mongoConnectionString(properties.getProperty("mongo.connection-string"))
        .mongoDatabase(properties.getProperty("mongo.database"))
        .mongoDatabase(properties.getProperty("mongo.collection"))
        .build();
    run(config);
  }
}
