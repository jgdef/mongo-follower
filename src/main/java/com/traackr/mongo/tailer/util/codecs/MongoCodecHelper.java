/**
 * MongoCodecHelper.java - Traackr, Inc.
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
