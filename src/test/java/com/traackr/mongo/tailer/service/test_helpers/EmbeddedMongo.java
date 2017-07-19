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

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;

import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.MongosExecutable;
import de.flapdoodle.embed.mongo.MongosProcess;
import de.flapdoodle.embed.mongo.MongosStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.IMongosConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.MongosConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;

import org.bson.Document;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author wwinder
 * Created on: 7/17/17
 */
public class EmbeddedMongo {
  private final String host = "localhost";
  private final int port;

  private EmbeddedMongo() throws IOException {
    port = Network.getFreeServerPort();
  }

  MongodExecutable mongodExecutable = null;
  public MongodProcess mongod;

  public MongoClient getClient() {
    return new MongoClient(host, port);
  }

  /**
   * Simple example to make sure the flapdoodle mongo wrapper works.
   */
  public static EmbeddedMongo simpleStartMongo(Version.Main version) throws IOException {
    EmbeddedMongo em = new EmbeddedMongo();

    MongodStarter starter = MongodStarter.getDefaultInstance();

    IMongodConfig mongodConfig = new MongodConfigBuilder()
        .version(version)
        .net(new Net(em.host, em.port, Network.localhostIsIPv6()))
        .build();

    em.mongodExecutable = starter.prepare(mongodConfig);
    em.mongod = em.mongodExecutable.start();

    return em;
  }

  /**
   * Start mongo with oplog / replicaset enabled.
   */
  public static EmbeddedMongo replicaSetStartMongo(Version.Main version) throws IOException {
    final EmbeddedMongo em = new EmbeddedMongo();

    em.mongod = startMongod(version, em.port);

    try (final MongoClient client = em.getClient()) {
      final Document host0 = new Document("_id", 0)
          .append("host", String.format("%s:%d", em.host, em.port));
      final List<Document> members = new ArrayList<>();
      members.add(host0);
      final Document replSetSettings = new Document("_id", "testRepSet")
          .append("members", members);
      final MongoDatabase adminDb = client.getDatabase("admin");

      adminDb.runCommand(new Document("replSetInitiate", replSetSettings));
    }

    try (MongoClient mongoClient = em.getClient()) {
      System.out.println("DB Names: " + mongoClient.listDatabaseNames());
    }

    return em;
  }

  private static MongosProcess startMongos(
      Version.Main version, int port, int defaultConfigPort, String defaultHost) throws
                                                                                 IOException {
    IMongosConfig mongosConfig = new MongosConfigBuilder()
        .version(version)
        .net(new Net(port, Network.localhostIsIPv6()))
        .configDB(defaultHost + ":" + defaultConfigPort)
        .replicaSet("testRepSet")
        .build();

    MongosExecutable mongosExecutable = MongosStarter.getDefaultInstance().prepare(mongosConfig);
    return mongosExecutable.start();
  }

  private static MongodProcess startMongod(Version.Main version, int defaultConfigPort)
      throws IOException {
    IMongodConfig mongoConfigConfig = new MongodConfigBuilder()
        .version(version)
        .net(new Net(defaultConfigPort, Network.localhostIsIPv6()))
        .replication(new Storage(null, "testRepSet", 500))
        .build();

    MongodExecutable mongodExecutable = MongodStarter.getDefaultInstance()
        .prepare(mongoConfigConfig);
    return mongodExecutable.start();
  }


  public void stopMongo() {
    if (mongodExecutable != null) {
      mongodExecutable.stop();
    }
    mongodExecutable = null;
  }
}
