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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.bson.BsonWriter;
import org.bson.codecs.DateCodec;
import org.bson.codecs.EncoderContext;

/**
 * @author wwinder
 *         Created on: 5/27/16
 */
public class CustomDateCodec extends DateCodec {
    private final SimpleDateFormat formatter;
    public CustomDateCodec(String format) {
        formatter = new SimpleDateFormat(format);
    }

    @Override
    public void encode(final BsonWriter writer, final Date value, final EncoderContext encoderContext) {
        writer.writeString(formatter.format(value));
    }
}
