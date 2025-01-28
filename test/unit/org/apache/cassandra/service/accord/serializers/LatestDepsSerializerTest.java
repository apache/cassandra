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

package org.apache.cassandra.service.accord.serializers;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import accord.primitives.LatestDeps;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import org.apache.cassandra.net.MessagingService.Version;

import static org.apache.cassandra.net.MessagingService.Version.VERSION_51;

public class LatestDepsSerializerTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final List<Version> SUPPORTED_VERSIONS = VERSION_51.greaterThanOrEqual();

    @Test
    public void emptySerializerTest() throws Throwable
    {
        for (Version ver : SUPPORTED_VERSIONS)
        {
            ByteBuffer bb = null;
            try (DataOutputBuffer buf = new DataOutputBuffer())
            {
                long expected = LatestDepsSerializers.latestDeps.serializedSize(LatestDeps.EMPTY, ver.value);
                LatestDepsSerializers.latestDeps.serialize(LatestDeps.EMPTY, buf, ver.value);
                bb = buf.asNewBuffer();
                Assert.assertEquals(expected, bb.capacity());
            }

            try (DataInputBuffer in = new DataInputBuffer(bb, false))
            {
                LatestDeps roundTrip = LatestDepsSerializers.latestDeps.deserialize(in, ver.value);
                Assert.assertEquals(roundTrip, LatestDeps.EMPTY);
            }
        }
    }
}