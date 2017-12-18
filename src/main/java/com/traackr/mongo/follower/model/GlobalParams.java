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

import com.traackr.mongo.follower.util.KillSwitch;

import org.bson.types.BSONTimestamp;

import lombok.Data;
import lombok.Getter;

/**
 * @author wwinder
 *         Created on: 5/29/16
 */
@Data
@Getter
public class GlobalParams {
  public GlobalParams(KillSwitch running, BSONTimestamp oplogTime,
                      int oplogDelay, int oplogInterval, String dateFormat, Boolean longToString) {
    this.running = running;
    this.oplogTime = oplogTime;
    this.oplogDelay = oplogDelay;
    this.oplogInterval = oplogInterval;
    this.dateFormat = dateFormat;
    this.longToString = longToString;
  }

  public KillSwitch running;
  public BSONTimestamp oplogTime;
  public Integer oplogDelay;
  public Integer oplogInterval;
  public String dateFormat;
  public Boolean longToString;
}
