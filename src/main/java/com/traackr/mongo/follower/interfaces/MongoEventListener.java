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
package com.traackr.mongo.follower.interfaces;

import com.traackr.mongo.follower.model.Command;
import com.traackr.mongo.follower.model.Delete;
import com.traackr.mongo.follower.model.Insert;
import com.traackr.mongo.follower.model.OplogEntry;
import com.traackr.mongo.follower.model.Update;

import org.bson.Document;

public interface MongoEventListener {
  /**
   * Dispatch the latest oplog entry to the listener
   *
   * @param entry
   */
    default void dispatch(final OplogEntry entry) {
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
   * Initial export documents come through this event.
   *
   * @param doc document being exported.
   */
  void exportDocument(Document doc);

  void delete(Delete entry);

  void insert(Insert entry);

  void update(Update entry);

  void command(Command entry);
}
