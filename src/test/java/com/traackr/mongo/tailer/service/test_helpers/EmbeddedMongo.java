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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

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
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Date;

/**
 * @author wwinder
 *         Created on: 7/17/17
 */
public class EmbeddedMongo {
  private EmbeddedMongo() {}

  MongodExecutable mongodExecutable = null;
  public MongodProcess mongod;
  MongosExecutable mongosExecutable = null;
  public MongosProcess mongos;

  public MongoClient mongo;
  public DB db;
  public DBCollection col;

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
    em.db = em.mongo.getDB("test");
    em.col = em.db.createCollection("testCol", new BasicDBObject());
    em.col.save(new BasicDBObject("testDoc", new Date()));

    return em;
  }

  public static EmbeddedMongo replicaSetStartMongo(Version.Main version) throws IOException {
    String dbdir = Files.createTempDirectory("test").toFile().getAbsolutePath();

    int mongosPort = Network.getFreeServerPort();
    int mongodConfigPort = Network.getFreeServerPort();
    String defaultHost = "localhost";

    EmbeddedMongo em = new EmbeddedMongo();

    em.mongod = startMongod(version, mongodConfigPort);

    // init replica set, aka rs.initiate()
    try (MongoClient client=new MongoClient(defaultHost, mongodConfigPort)) {
      client.getDatabase("admin").runCommand(new Document("replSetInitiate", new Document()));
    }

    em.mongos = startMongos(version, mongosPort, mongodConfigPort, defaultHost);

    try (MongoClient mongoClient = new MongoClient(defaultHost, mongodConfigPort)) {
      System.out.println("DB Names: " + mongoClient.getDatabaseNames());
    }

    // Initialize some client stuffs.
    em.mongo = new MongoClient(defaultHost, mongosPort);
    em.db = em.mongo.getDB("testttt");

    try {
      Thread.sleep(5000);
    } catch (Exception e) {}
    //em.col = em.db.getCollection("testtttCol");
    em.col = em.db.createCollection("testtttCol", new BasicDBObject());
    em.col.save(new BasicDBObject("testDoc", new Date()));

    return em;
  }

  private static MongosProcess startMongos(Version.Main version, int port, int defaultConfigPort, String defaultHost) throws
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

  private static MongodProcess startMongod(Version.Main version, int defaultConfigPort) throws IOException {
    IMongodConfig mongoConfigConfig = new MongodConfigBuilder()
        .version(version)
        .net(new Net(defaultConfigPort, Network.localhostIsIPv6()))
        .replication(new Storage(null, "testRepSet", 5000))
        .configServer(true)
        .build();

    MongodExecutable mongodExecutable = MongodStarter.getDefaultInstance().prepare(mongoConfigConfig);
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
