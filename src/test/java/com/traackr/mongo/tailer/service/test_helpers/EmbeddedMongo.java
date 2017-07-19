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
  private EmbeddedMongo() {
  }

  MongodExecutable mongodExecutable = null;
  public MongodProcess mongod;
  public MongoClient mongo;
  public MongoDatabase db;
  public MongoCollection<Document> col;

  // Simple example.
  public static EmbeddedMongo simpleStartMongo(Version.Main version) throws IOException {
    EmbeddedMongo em = new EmbeddedMongo();

    MongodStarter starter = MongodStarter.getDefaultInstance();

    String bindIp = "localhost";
    int port = 12345;
    IMongodConfig mongodConfig = new MongodConfigBuilder()
        .version(version)
        .net(new Net(bindIp, port, Network.localhostIsIPv6()))
        .build();

    em.mongodExecutable = starter.prepare(mongodConfig);
    em.mongod = em.mongodExecutable.start();

    em.mongo = new MongoClient(bindIp, port);
    em.db = em.mongo.getDatabase("test");
    em.db.createCollection("testCol", new CreateCollectionOptions());
    em.col = em.db.getCollection("testCol");
    em.col.insertOne(new Document("testDoc", new Date()));

    return em;
  }

  public static EmbeddedMongo replicaSetStartMongo(Version.Main version) throws IOException {
    final String host = "localhost";
    final int port = Network.getFreeServerPort();
    final EmbeddedMongo em = new EmbeddedMongo();

    em.mongod = startMongod(version, port);

    final ServerAddress primaryAddr = new ServerAddress(host, port);
    try (final MongoClient client = new MongoClient(primaryAddr)) {
      final Document host0 = new Document("_id", 0)
          .append("host", String.format("%s:%d", host, port));
      final List<Document> members = new ArrayList<>();
      members.add(host0);
      final Document replSetSettings = new Document("_id", "testRepSet")
          .append("members", members);
      final MongoDatabase adminDb = client.getDatabase("admin");

      adminDb.runCommand(new Document("replSetInitiate", replSetSettings));
    }

    try (MongoClient mongoClient = new MongoClient(primaryAddr)) {
      System.out.println("DB Names: " + mongoClient.listDatabaseNames());
    }

    // Initialize some client stuffs.
    em.mongo = new MongoClient(primaryAddr);
    em.db = em.mongo.getDatabase("testttt");

    try {
      Thread.sleep(5000);
    } catch (Exception e) {
    }
    em.db.createCollection("testtttCol", new CreateCollectionOptions());
    em.col = em.db.getCollection("testtttCol");
    em.col.insertOne(new Document("testDoc", new Date()));

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
    /*
    if (mongosExecutable != null) {
      mongosExecutable.stop();
    }
    mongosExecutable = null;

    if (mongodExecutable != null) {
      mongodExecutable.stop();
    }
    mongodExecutable = null;
    */
  }
}
