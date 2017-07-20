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
import com.traackr.mongo.tailer.model.OplogTimestampWriter;
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
    // Initialize oplog dir.
    try {
      Path parentDirectory = Paths.get(config.getOplogFile()).getParent();
      Files.createDirectories(parentDirectory);
    }
    catch (IOException ioe) {
      throw new FailedToStartException("Failed to find or create oplog file.", ioe);
    }

    // Global properties.
    GlobalParams globalParams = new GlobalParams(
        config.getDryRun(),
        new KillSwitch(),
        getOplogTimestamp(config.getOplogFile()),
        config.getOplogDelayMinutes(),
        config.getOplogUpdateIntervalMinutes(),
        null,
        false
    );

    // Mongo properties
    OpLogTailerParams mongoParams = null;
      mongoParams = OpLogTailerParams.with(
          globalParams,
          config.getInitialImport(),
          config.getQueue(),
          config.getMongoConnectionString(),
          config.getMongoDatabase(),
          config.getMongoCollection());

    // Initialize OpLogTail
    MongoReader oplogTailer = new MongoReader(mongoParams);

    // Initialize OpLogProcessor
    OpLogProcessor oplogProcessor = new OpLogProcessor(globalParams, config.getQueue(), config.getListener());

    // Oplog writer
    OplogTimestampWriter oplogWriter = new OplogTimestampWriter(globalParams);

    ///////////////////
    // Start threads //
    ///////////////////
    launchThreads(config.getQueue(), oplogTailer, oplogProcessor, oplogWriter);
  }

  private static void launchThreads(
                             final BlockingQueue<Record> queue,
                             MongoReader oplogTailer,
                             OpLogProcessor oplogProcessor,
                             OplogTimestampWriter oplogWriter) {
    final ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 4,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());

    pool.submit(oplogTailer);
    pool.submit(oplogProcessor);
    pool.submit(oplogWriter);

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
    try {
      TailerConfig config = TailerConfig.builder()
          .listener(listener)
          .queue(new ArrayBlockingQueue<>(Integer.valueOf(properties.getProperty("queue-size"))))
          .dryRun(Boolean.valueOf(properties.getProperty("dry-run")))
          .oplogFile(properties.getProperty("oplog-file"))
          .oplogDelayMinutes(Integer.valueOf(properties.getProperty("mongo.oplog-delay")))
          .oplogUpdateIntervalMinutes(Integer.valueOf(properties.getProperty("mongo.oplog-interval")))
          .initialImport(Boolean.valueOf(properties.getProperty("initial-import")))
          .mongoConnectionString(properties.getProperty("mongo.connection-string"))
          .mongoDatabase(properties.getProperty("mongo.database"))
          .mongoCollection(properties.getProperty("mongo.collection"))
          .build();
      run(config);
    } catch (Exception e) {
      throw new FailedToStartException("Problem initializing configuration.", e);
    }
  }
}
