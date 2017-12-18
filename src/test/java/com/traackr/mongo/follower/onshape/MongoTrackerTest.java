package com.traackr.mongo.follower.onshape;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;
import java.util.Set;

import org.bson.Document;
import org.springframework.util.StringUtils;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.collect.ImmutableSet;
import com.traackr.mongo.follower.exceptions.FailedToStartException;
import com.traackr.mongo.follower.interfaces.MongoEventListener;
import com.traackr.mongo.follower.model.Command;
import com.traackr.mongo.follower.model.Delete;
import com.traackr.mongo.follower.model.FollowerConfig;
import com.traackr.mongo.follower.model.Insert;
import com.traackr.mongo.follower.model.OplogEntry;
import com.traackr.mongo.follower.model.Update;
import com.traackr.mongo.follower.service.CassandraSink;
import com.traackr.mongo.follower.service.OpLogSinkAndSource;
import com.traackr.mongo.follower.service.Runner;


public class MongoTrackerTest implements MongoEventListener {
  private static final Logger logger = Logger.getLogger(MongoTrackerTest.class.getName());
  private static boolean cassandraSinkUnitTest = false;
  private static HelenusSession hs_;
  
  /*  
  static { 	  
      Cluster cluster = getCluster();
      HelenusSession helenusSession = Helenus.connect()
          //.add(BTBillingPlan.class)
          //.add(BTUser.class)
          //.add(BTBillingAccount.class)
          .addPackage("com.belmonttech.model")
          .autoCreate()
          .setUnitOfWorkClass(BTUnitOfWork.class)
          .consistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
          .withCachingExecutor()
          //.metricRegistry(metricRegistry_)
          //.zipkinTracer(traceService_.getTracer())
          .get();
      setupMetrics(cluster, getKeyspaceName());      
  }
*/

  public static void main(String [] args) {
	checkArgs(args);    // may throw and exit
    if (cassandraSinkUnitTest) {
	    cassandraSinkUnitTest();
	    System.exit(0);
    }

	Path opLogDirectory = Paths.get(args[1]);
	if (!Files.isDirectory(opLogDirectory, LinkOption.NOFOLLOW_LINKS)) {
		throw new IllegalArgumentException(args[1] + " is not a valid directory");
	}

    Set<String> dbs = StringUtils.commaDelimitedListToSet(args[2]);
    
    OpLogSinkAndSource sinkAndSource = null;
    try {
        @SuppressWarnings("unchecked")
			Class<? extends OpLogSinkAndSource> cls = (Class<? extends OpLogSinkAndSource>) Class
					.forName("com.traackr.mongo.follower.service.InMemoryOpLogSinkAndSource");
        sinkAndSource = cls.newInstance();
    } catch(Throwable t) {
    	logger.severe("Unable to instantiate oplog sink");
    	t.printStackTrace(System.err);
    	System.exit(-1);;
    }
    
    MongoTrackerTest self = new MongoTrackerTest();    
    FollowerConfig fc = FollowerConfig.builder()
      .listener(self)
      .initialExport(false)
      .mongoConnectionString(args[0])
      .mongoDatabases(dbs)
      .mongoCollections(ImmutableSet.of()) // prefer to list databases
      .oplogDelayMinutes(1)
      .oplogFile(opLogDirectory)
      .oplogUpdateIntervalMinutes(10)
      .oplogSink(sinkAndSource)
      .oplogSource(sinkAndSource)
      .build();    

    try {
        Runner.run(fc);
    }
    catch (FailedToStartException ftse) {
        ftse.printStackTrace(System.err);
    }
  }

  @Override
  public void exportDocument(Document entry) {
	logger.fine("Export not supported here");
  }
  
  @Override
  public void delete(Delete entry) {
    logger.fine("Delete: " + entry.getId());
    process(entry);
  }

  @Override
  public void insert(Insert entry) {
	logger.fine("Insert: " + entry.getDocument().toString());
    process(entry);
  }

  @Override
  public void update(Update entry) {
	logger.fine("Update: " + entry.getDocument().toString());
    process(entry);
  }

  @Override
  public void command(Command entry) {
	logger.fine("Command: " + entry.toString());
    process(entry);
  }
  
  private void process(OplogEntry entry) {
	try {
	   CassandraSink.process(entry);
	} catch (Exception e) {
	 	logger.warning("Cassandra sink failed to process insert oplog entry");
	    e.printStackTrace(System.err);
	}
  }
  
  private static void checkArgs(String[] args) {
	  if (args == null || args.length < 4) {
		  throw new IllegalArgumentException("Arguments: <conn str> <oplog dir> <dbs> <collection>");
	  }
  }
  
  private static void cassandraSinkUnitTest() {
      try {
          @SuppressWarnings("unchecked")
		  Class<? extends CassandraSink> c = (Class<? extends CassandraSink>) Class.forName("com.traackr.mongo.follower.service.CassandraSink"); 
	      org.bson.Document doc = org.bson.Document.parse("{\"op\":\"i\", \"v\":2, \"ts\":{\"$timestamp\": {\"t\":1505398435,\"i\":1}}, \"ns\":\"coredb.btuser\", \"o\":{\"a\":1, \"b\":2}}");
  	      com.traackr.mongo.follower.model.OplogEntry entry = com.traackr.mongo.follower.model.OplogEntry.of(doc);
	      java.lang.reflect.Method m = c.getMethod("process", com.traackr.mongo.follower.model.OplogEntry.class);
	      m.invoke(null, entry);
	      System.exit(0);
	  } catch (Throwable t) {
		  System.exit(-1);
      }
  }
}
