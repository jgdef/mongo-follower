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
package com.traackr.mongo.follower.model;

import com.traackr.mongo.follower.interfaces.MongoEventListener;
import com.traackr.mongo.follower.service.OpLogSink;
import com.traackr.mongo.follower.service.OpLogSource;

import java.nio.file.Path;
import java.util.Set;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * @author wwinder
 *         Created on: 7/18/17
 */
@Data
@Builder
public class FollowerConfig {
  /**
   * How many minutes to lag behind the oplog to allow for overlapping events in case of restart
   * instead of lost events.
   */
  @NonNull
  @Builder.Default
  Integer oplogDelayMinutes = 10;

  /**
   * How frequently to update the oplog.
   */
  @NonNull
  @Builder.Default
  Integer oplogUpdateIntervalMinutes = 10;

  /**
   * The oplogSink implementation.
   */
  @NonNull
  @Builder.Default
  OpLogSink oplogSink;

  /**
   * The oplogSource implementation.
   */
  @NonNull
  @Builder.Default
  OpLogSource oplogSource;
  
  /**
   * The callback handler.
   */
  @NonNull
  MongoEventListener listener;

  /**
   * Location of oplog file.
   */
  @NonNull
  Path oplogFile;

  /**
   * Mongo connection string.
   */
  @NonNull
  String mongoConnectionString;

  /**
   * Which database(s) to tail.
   */
  @NonNull
  Set<String> mongoDatabases;

  /**
   * Which collection(s) to tail.
   */
  @NonNull
  Set<String> mongoCollections;

  /**
   * Whether to do an initial export on the collection before oplog tailing.
   * TODO: Separate process?
   */
  @NonNull
  @Builder.Default
  Boolean initialExport = false;
}
