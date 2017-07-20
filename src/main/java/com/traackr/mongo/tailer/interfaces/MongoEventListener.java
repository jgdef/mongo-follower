/**
 * MongoEventListener.java - Traackr, Inc.
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

package com.traackr.mongo.tailer.interfaces;

import com.traackr.mongo.tailer.model.OplogLine;

import org.bson.Document;

/**
 * @author wwinder
 *         Created on: 12/8/16
 */
public interface MongoEventListener {

  /**
   * Initial import events come through this event.
   * @param doc record being imported.
   */
  void importRecord(Document doc);

  /**
   * Oplog delete event.
   * @param id
   * @param oplog
   */
  void delete(String id, OplogLine oplog);

  /**
   * @param doc Document being inserted
   */
  void insert(Document doc, OplogLine oplog);

  /**
   * @param replace Whether this update completely replaces the existing document
   */
  void update(boolean replace, OplogLine doc);

  void command(Document doc, OplogLine oplog);
}
