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

import com.traackr.mongo.tailer.model.Command;
import com.traackr.mongo.tailer.model.Delete;
import com.traackr.mongo.tailer.model.Insert;
import com.traackr.mongo.tailer.model.OplogEntry;
import com.traackr.mongo.tailer.model.Update;

import org.bson.Document;

/**
 * @author wwinder
 *         Created on: 12/8/16
 */
public interface MongoEventListener {
  default void process(final OplogEntry entry) {
    if (entry instanceof Insert) {
      insert((Insert) entry);
    } else if (entry instanceof Update) {
      update((Update) entry);
    } else if (entry instanceof Delete) {
      delete((Delete) entry);
    } else if (entry instanceof Command) {
      command((Command) entry);
    }
  }

  /**
   * Initial import documents come through this event.
   *
   * @param doc document being imported.
   */
  void importDocument(Document doc);

  void delete(Delete entry);

  void insert(Insert entry);

  void update(Update entry);

  void command(Command entry);
}
