/*
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

package org.apache.cassandra.utils;

import java.io.IOException;

import org.junit.Test;

import accord.utils.Gens;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.utils.Property.qt;
import static org.apache.cassandra.io.Serializers.testSerde;
import static org.apache.cassandra.utils.CollectionSerializers.newListSerializer;

public class CollectionSerializersTest
{
    @Test
    public void serde()
    {
        // This test is testing the collection serializer and not the serializer it uses for the element. There are
        // special things that must be accounted for in the test
        // 1) number of elements can hit the different byte counts (vint can be 1-4 bytes)
        // 2) element serializer needs to be fast.  So this test avoids random values for the element
        DataOutputBuffer output = new DataOutputBuffer();
        Integer cached = 42;
        //       0 takes 1 bytes
        //     128 takes 2 bytes
        //   16384 takes 3 bytes
        // 2097152 takes 4 bytes
        qt().forAll(Gens.lists(i -> cached).ofSizeBetween(0, 2_097_152)).check(list -> {
            testSerde(output, newListSerializer((UnversionedSerializer<Integer>) IntSerializer.instance), list);
            testSerde(output, newListSerializer((IVersionedSerializer<Integer>) IntSerializer.instance), list, 0);
            testSerde(output, newListSerializer((VersionedSerializer<Integer, Version>) IntSerializer.instance), list, Version.V1);
        });
    }

    public enum Version
    {
        V1
    }

    public enum IntSerializer implements UnversionedSerializer<Integer>, IVersionedSerializer<Integer>, VersionedSerializer<Integer, Version>
    {
        instance;

        @Override
        public void serialize(Integer t, DataOutputPlus out) throws IOException
        {
            out.writeInt(t);
        }

        @Override
        public void serialize(Integer t, DataOutputPlus out, Version version) throws IOException
        {
            serialize(t, out);
        }

        @Override
        public void serialize(Integer t, DataOutputPlus out, int version) throws IOException
        {
            serialize(t, out);
        }

        @Override
        public Integer deserialize(DataInputPlus in) throws IOException
        {
            return in.readInt();
        }

        @Override
        public Integer deserialize(DataInputPlus in, Version version) throws IOException
        {
            return deserialize(in);
        }

        @Override
        public Integer deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in);
        }

        @Override
        public long serializedSize(Integer t)
        {
            return TypeSizes.INT_SIZE;
        }

        @Override
        public long serializedSize(Integer t, Version version)
        {
            return serializedSize(t);
        }

        @Override
        public long serializedSize(Integer t, int version)
        {
            return serializedSize(t);
        }
    }
}