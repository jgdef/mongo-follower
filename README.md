# Mongo Follower

A simple, robust and flexible interface for streaming data out of MongoDB.

## Key features
* **Resumable**. An oplog timestamp is maintained which allows for restarting `MongoFollower` to resume processing. The timestamp can be configured with:
  * `updateInterval`: control how often the file is updated.
  * `updateDelay`: subtracts some amount of time from the oplog so that oplog events aren't skipped during a restart.
* **Initial import**. By starting the process with an initial import all documents can be exported effortlessly.
* **Runner harness**. The `Runner` utilities make setting up a follower a breeze.

## Options

| builderOption | type | default value | description |
| ------------- | ---- | ------------- | ----------- |
|`listener` | MongoEventListener | --- | A class extending the `MongoEventListener` interface to process events. |
|`initialImport` | Boolean | false | Toggle whether or not an initial import should be performed. |
|`oplogFile` | String | --- | The absolute path to the oplog file, this needs to be accessible for reading and writing by the user running MongoFollower. |
|`mongoConnectionString` | String | --- | Standard MongoDB connection string. |
|`mongoDatabase` | String | --- | Database containing the collection to be followed. |
|`mongoCollection` | String | --- | Collection being followed. |
| `oplogDelayMinutes` | int | 10 | Number of minutes to lag behind the oplog. By delaying the oplog you can restart your process without missing any events. Note that this expects that it is ok to send the same event multiple times as long as they are sent in order. |
|`oplogUpdateIntervalMinutes` | int | 10 | The number of minutes to wait between updating the oplog timestamp file. |
|`queue` | BlockingQueue<Record> | ArrayBlockingQueue<>(4000) | Optionally override the queue implementation with something custom or with a different capacity. |


## How it works
The mongo follower is a two step process to efficiently export data from a collection then keep it synchronized.

1. An initial export gets the bulk of your historic documents out of the collection. This can be disabled by setting `initialImport` to `false`.
2. A MongoDB oplog tailing process is started which keeps processing events as they occur.

## Example

Here is a complete working example which will process all documents of a given collection specified on the command line. To process all documents from a given instance, database, collection:
```
java -jar TestApp.jar mongodb://localhost:27017 test_database test_collection
```

```java
import com.traackr.mongo.tailer.exceptions.FailedToStartException;
import com.traackr.mongo.tailer.interfaces.MongoEventListener;
import com.traackr.mongo.tailer.model.Command;
import com.traackr.mongo.tailer.model.Delete;
import com.traackr.mongo.tailer.model.Insert;
import com.traackr.mongo.tailer.model.TailerConfig;
import com.traackr.mongo.tailer.model.Update;
import com.traackr.mongo.tailer.service.Runner;

import org.bson.Document;

import java.io.IOException;

/**
 * @author wwinder
 * Created on: 7/20/17
 */
public class TestApp implements MongoEventListener {
  public static void main(String[] args)
      throws IOException, FailedToStartException, InterruptedException {
    TestApp listener = new TestApp();

    if (args.length != 3) {
      throw new IllegalArgumentException("Arguments: <connection string> <database> <collection>");
    }

    String connectionString = args[0];
    String database = args[1];
    String collection = args[2];

    TailerConfig tc = TailerConfig.builder()
        .listener(listener)
        .dryRun(false)
        .initialImport(false)
        .mongoConnectionString(connectionString)
        .mongoDatabase(database)
        .mongoCollection(collection)
        .oplogFile("/tmp/testapp/oplogfile")
        .build();

    Runner.run(tc);

    while (true) {
      Thread.sleep(1000000);
    }
  }

  @Override
  public void importDocument(Document doc) {
    System.out.println("Import: " + doc.toString());
  }

  @Override
  public void delete(Delete entry) {
    System.out.println("Delete: " + entry.getId());
  }

  @Override
  public void insert(Insert entry) {
    System.out.println("Delete: " + entry.getDocument().toString());
  }

  @Override
  public void update(Update entry) {
    System.out.println("Update: " + entry.getDocument().toString());
  }

  @Override
  public void command(Command entry) {
    System.out.println("Command: " + entry.toString());
  }
}
```
