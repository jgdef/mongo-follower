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
package com.traackr.mongo.tailer.util.codecs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonType;
import org.bson.Document;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.Encoder;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import com.mongodb.MongoClient;

/**
 * @author wwinder
 *         Created on: 6/17/16
 */
public class MongoCodecHelper {
  /**
   * Customizations for the document.toJson output.
   *
   * http://mongodb.github.io/mongo-java-driver/3.0/bson/codecs/
   *
   * @return the toJson encoder.
   */
  public static Encoder<Document> getEncoder(CodecParams params) {
    Map<BsonType, Class<?>> replacements = new HashMap<BsonType, Class<?>>();
    ArrayList<Codec<?>> codecs = new ArrayList<>();

    if (params.dateFormat != null) {
      // Replace default DateCodec class to use the custom date formatter.
      replacements.put(BsonType.DATE_TIME, CustomDateCodec.class);
      codecs.add(new CustomDateCodec(params.dateFormat));
    }

    if (params.longToString) {
      // Replace default LongCodec class
      replacements.put(BsonType.INT64, CustomLongCodec.class);
      codecs.add(new CustomLongCodec());
    }

    if (codecs.size() > 0) {
      BsonTypeClassMap bsonTypeClassMap = new BsonTypeClassMap(replacements);
      DocumentCodecProvider documentCodecProvider = new DocumentCodecProvider(bsonTypeClassMap);

      CodecRegistry codecRegistry = codecRegistry = CodecRegistries.fromRegistries(
          CodecRegistries.fromCodecs(codecs),
          CodecRegistries.fromProviders(documentCodecProvider),
          MongoClient.getDefaultCodecRegistry());

      return new DocumentCodec(codecRegistry, bsonTypeClassMap);
    } else {
      return new DocumentCodec();
    }
  }
}
