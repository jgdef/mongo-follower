/**
 * OplogTimestampHelper.java - Traackr, Inc.
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
package com.traackr.mongo.tailer.util;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author wwinder
 *         Created on: 6/17/16
 */
public class OplogTimestampHelper {
  private static final Logger logger = LoggerFactory.getLogger(OplogTimestampHelper.class);

  private static Path OPLOG_FILE = null;

  public static String serializeBsonTimestamp(BSONTimestamp value) {
    return value.getTime() + "," + value.getInc();
  }

  synchronized public static void saveOplogTimestamp(BSONTimestamp value) throws Exception {
    if (!Files.exists(OPLOG_FILE)) {
      Files.createFile(OPLOG_FILE);
    }

    String serialized = serializeBsonTimestamp(value);

    Files.write(OPLOG_FILE, serialized.getBytes());
  }

  synchronized public static BSONTimestamp getOplogTimestamp(String path) {
    BSONTimestamp ret = new BSONTimestamp(0,0);
    OPLOG_FILE = Paths.get(path);
    boolean fileDataExists = false;

    // Attempt to read file.
    try {
      if (Files.exists(OPLOG_FILE)) {
        String parts[] = new String(Files.readAllBytes(OPLOG_FILE)).split(",");
        logger.info("Read oplog.timestamp: " + parts[0] + ", " + parts[1]);
        if (parts.length == 2) {
          fileDataExists = true;
        }

        ret = new BSONTimestamp(Integer.valueOf(parts[0].trim()), Integer.valueOf(parts[1].trim()));
        logger.info("oplog.timestamp set to: " + ret);
      }
    } catch (Exception e) {
      // This probably happened when manually recovering the oplog timestamp.
      if (fileDataExists) {
        logger.error("Invalid oplog timestamp!", e);
        System.exit(-1);
      }

      // Unable to get an existing oplog.
      logger.error("Unable to find a valid timestamp in '"
          + OPLOG_FILE.toAbsolutePath() + "', if this isn't the " +
          "first run that might be a problem.", e);
    }

    return ret;
  }
}
