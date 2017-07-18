/**
 * OplogTimestampHelperTest.java - Traackr, Inc.
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
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * @author Michael McLellan
 *         Created on: 6/28/16
 */
public class OplogTimestampHelperTest {
  /**
   * Create, Read, and Update timestamp file
   */
  @Test
  public void handleTimestampFile() throws Exception {
    final String random = UUID.randomUUID().toString();
    final String tmp = Files.createTempDirectory(null).toString();
    final Path timestampFile = Paths.get(tmp, random);
    final BSONTimestamp initialTimestamp = OplogTimestampHelper.getOplogTimestamp(timestampFile.toString());
    Assert.assertFalse(Files.exists(timestampFile));
    Assert.assertEquals(initialTimestamp, new BSONTimestamp(0, 0));

    final BSONTimestamp expected = new BSONTimestamp(1413782642, 5);
    OplogTimestampHelper.saveOplogTimestamp(expected);
    Assert.assertTrue(Files.exists(timestampFile));

    final BSONTimestamp actual = OplogTimestampHelper.getOplogTimestamp(timestampFile.toString());
    Assert.assertEquals(expected, actual);
  }
}
