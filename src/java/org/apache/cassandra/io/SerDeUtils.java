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

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.ipc.ByteBufferInputStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.cassandra.io.util.OutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Static serialization/deserialization utility functions, intended to eventually replace IVersionedSerializers.
 */
public final class SerDeUtils
{
    // unbuffered decoders
    private final static DecoderFactory DIRECT_DECODERS = new DecoderFactory().configureDirectDecoder(true);

	/**
     * Deserializes a single object based on the given Schema.
     * @param writer writer's schema
     * @param bytes Array to deserialize from
     * @param ob An empty object to deserialize into (must not be null).
     * @throws IOException
     */
    public static <T extends SpecificRecord> T deserialize(Schema writer, ByteBuffer bytes, T ob) throws IOException
    {
        BinaryDecoder dec = DIRECT_DECODERS.createBinaryDecoder(ByteBufferUtil.getArray(bytes), null);
        SpecificDatumReader<T> reader = new SpecificDatumReader<T>(writer);
        reader.setExpected(ob.getSchema());
        return reader.read(ob, dec);
    }

	/**
     * Serializes a single object.
     * @param o Object to serialize
     */
    public static <T extends SpecificRecord> ByteBuffer serialize(T o) throws IOException
    {
        OutputBuffer buff = new OutputBuffer();
        BinaryEncoder enc = new BinaryEncoder(buff);
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(o.getSchema());
        writer.write(o, enc);
        enc.flush();
        return ByteBuffer.wrap(buff.asByteArray());
    }

	/**
     * Deserializes a single object as stored along with its Schema by serialize(T). NB: See warnings on serialize(T).
     * @param ob An empty object to deserialize into (must not be null).
     * @param bytes Array to deserialize from
     * @throws IOException
     */
    public static <T extends SpecificRecord> T deserializeWithSchema(ByteBuffer bytes, T ob) throws IOException
    {
        BinaryDecoder dec = DIRECT_DECODERS.createBinaryDecoder(ByteBufferUtil.getArray(bytes), null);
        Schema writer = Schema.parse(dec.readString(new Utf8()).toString());
        SpecificDatumReader<T> reader = new SpecificDatumReader<T>(writer);
        reader.setExpected(ob.getSchema());
        return reader.read(ob, dec);
    }

	/**
     * Serializes a single object along with its Schema. NB: For performance critical areas, it is <b>much</b>
     * more efficient to store the Schema independently.
     * @param o Object to serialize
     */
    public static <T extends SpecificRecord> ByteBuffer serializeWithSchema(T o) throws IOException
    {
        OutputBuffer buff = new OutputBuffer();
        BinaryEncoder enc = new BinaryEncoder(buff);
        enc.writeString(new Utf8(o.getSchema().toString()));
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(o.getSchema());
        writer.write(o, enc);
        enc.flush();
        return ByteBuffer.wrap(buff.asByteArray());
    }

    /**
     * @return a DataInputStream wrapping the given buffer.
     */
    public static DataInputStream createDataInputStream(ByteBuffer buff)
    {
        ByteBufferInputStream bbis = new ByteBufferInputStream(Collections.singletonList(buff));
        return new DataInputStream(bbis);
    }

    /**
     * Create a generic array of the given type and size. Mostly to minimize imports.
     */
    public static <T> GenericArray<T> createArray(int size, Schema schema)
    {
        return new GenericData.Array<T>(size, Schema.createArray(schema));
    }
}
