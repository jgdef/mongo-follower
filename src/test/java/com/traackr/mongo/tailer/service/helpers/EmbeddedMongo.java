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
package com.traackr.mongo.tailer.service.helpers;

import static com.mongodb.client.model.Filters.gte;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

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

  public String getConnectionString() {
    return "mongodb://" + host + ":" + port;
  }

  /**
   * Helper that creates some documents.
   */
  public static void createDocuments(EmbeddedMongo em, String database, String collection,
                                     final int messageCount, final boolean withCleanup) {
    MongoDatabase db = em.getClient().getDatabase(database);
    MongoCollection<Document> col = db.getCollection(collection);


    if (withCleanup) {
      col.findOneAndDelete(gte("count", 0));
    }

    int count = messageCount;
    while (count-- > 0) {
      // TODO: count is int and makes id an Integer if we don't convert
      Document doc = new Document("name", "MongoDB")
          .append("type", "database")
          .append("info", new BasicDBObject("x", 203).append("y", 102))
          .append("_id", Integer.toString(count));
      col.insertOne(doc);
    }
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
    // This isn't always working.
    /*
    if (mongodExecutable != null) {
      mongodExecutable.stop();
    }
    mongodExecutable = null;
    */
  }
}
