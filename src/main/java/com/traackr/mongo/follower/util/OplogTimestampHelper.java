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
package com.traackr.mongo.follower.util;

import org.bson.types.BSONTimestamp;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author wwinder
 *         Created on: 6/17/16
 */
public class OplogTimestampHelper {
  private static final Logger logger = Logger.getLogger(OplogTimestampHelper.class.getName());

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
        logger.log(Level.SEVERE, "Invalid oplog timestamp!", e);
        System.exit(-1);
      }

      // Unable to get an existing oplog.
      logger.log(Level.SEVERE, "Unable to find a valid timestamp in '"
          + OPLOG_FILE.toAbsolutePath() + "', if this isn't the " +
          "first run that might be a problem.", e);
    }

    return ret;
  }
}
