/**
 * OpLogEventListener.java - Traackr, Inc.
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

import java.util.Collection;

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
   * @param wholesale this is an update being treated as an insert.
   */
  void insert(boolean wholesale, Document doc, OplogLine oplog);

  /**
   * Different approaches to an update...
   *
   * Elasticsearch-river-mondodb:
   * 1. Query mongo for the document being updated.
   * 2. Insert the results of that query.
   *
   * Mongo-connector:
   * 1. Query elasticsearch for the document being updated.
   * 2. Apply the update to the retrieved document.
   * 3. Delete the document from elasticsearch.
   * 4. Insert the new document as usual.
   */
  void update(OplogLine doc);

  /**
   * Bulk update a collection of documents by their id.
   */
  void bulkUpdate(Collection<OplogLine> docs);

  void command(Document doc, OplogLine oplog);
}
