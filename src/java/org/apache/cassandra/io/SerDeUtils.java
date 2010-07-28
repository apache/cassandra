/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;

import org.apache.cassandra.io.util.OutputBuffer;

/**
 * Static serialization/deserialization utility functions, intended to eventually replace ICompactSerializers.
 */
public final class SerDeUtils
{
    // unbuffered decoders
    private final static DecoderFactory DIRECT_DECODERS = new DecoderFactory().configureDirectDecoder(true);

    public static <T> Map<Utf8,T> toAvroMap(Map<String,T> map)
    {
        Map<Utf8,T> out = new HashMap<Utf8,T>();
        for (Map.Entry<String,T> entry : map.entrySet())
            out.put(new Utf8(entry.getKey()), entry.getValue());
        return out;
    }

    public static <T> Map<String,T> fromAvroMap(Map<Utf8,T> map)
    {
        Map<String,T> out = new HashMap<String,T>();
        for (Map.Entry<Utf8,T> entry : map.entrySet())
            out.put(entry.getKey().toString(), entry.getValue());
        return out;
    }

	/**
     * Deserializes a single object based on the given Schema.
     * @param schema writer's schema
     * @param bytes Array to deserialize from
     * @throws IOException
     */
    public static <T extends SpecificRecord> T deserialize(Schema schema, byte[] bytes) throws IOException
    {
        BinaryDecoder dec = DIRECT_DECODERS.createBinaryDecoder(bytes, null);
        return new SpecificDatumReader<T>(schema).read(null, dec);
    }

	/**
     * Serializes a single object.
     * @param o Object to serialize
     */
    public static <T extends SpecificRecord> byte[] serialize(T o) throws IOException
    {
        OutputBuffer buff = new OutputBuffer();
        BinaryEncoder enc = new BinaryEncoder(buff);
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(o.getSchema());
        writer.write(o, enc);
        enc.flush();
        return buff.asByteArray();
    }

	/**
     * Deserializes a single object as stored along with its Schema by serialize(T). NB: See warnings on serialize(T).
     * @param bytes Array to deserialize from
     * @throws IOException
     */
    public static <T extends SpecificRecord> T deserializeWithSchema(byte[] bytes) throws IOException
    {
        BinaryDecoder dec = DIRECT_DECODERS.createBinaryDecoder(bytes, null);
        Schema schema = Schema.parse(dec.readString(new Utf8()).toString());
        return new SpecificDatumReader<T>(schema).read(null, dec);
    }

	/**
     * Serializes a single object along with its Schema. NB: For performance critical areas, it is <b>much</b>
     * more efficient to store the Schema independently.
     * @param o Object to serialize
     */
    public static <T extends SpecificRecord> byte[] serializeWithSchema(T o) throws IOException
    {
        OutputBuffer buff = new OutputBuffer();
        BinaryEncoder enc = new BinaryEncoder(buff);
        enc.writeString(new Utf8(o.getSchema().toString()));
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(o.getSchema());
        writer.write(o, enc);
        enc.flush();
        return buff.asByteArray();
    }

    /**
     * Create a generic array of the given type and size. Mostly to minimize imports.
     */
    public static <T> GenericArray<T> createArray(int size, Schema schema)
    {
        return new GenericData.Array<T>(size, Schema.createArray(schema));
    }
}
