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

package org.apache.cassandra.db.streaming;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.streaming.StreamRequest;

public class StreamRequestTest
{
    private static InetAddressAndPort local;
    private final String ks = "keyspace";
    private final int version = MessagingService.current_version;

    @BeforeClass
    public static void setUp() throws Throwable
    {
        DatabaseDescriptor.daemonInitialization();
        local = InetAddressAndPort.getByName("127.0.0.1");
    }

    @Test
    public void serializationRoundTrip() throws Throwable
    {
        StreamRequest orig = new StreamRequest(ks,
                                               atEndpoint(Arrays.asList(range(1, 2), range(3, 4), range(5, 6)),
                                                          Collections.emptyList()),
                                               atEndpoint(Collections.emptyList(),
                                                          Arrays.asList(range(5, 6), range(7, 8))),
                                               Arrays.asList("a", "b", "c"));

        int expectedSize = (int) StreamRequest.serializer.serializedSize(orig, version);
        try (DataOutputBuffer out = new DataOutputBuffer(expectedSize))
        {
            StreamRequest.serializer.serialize(orig, out, version);
            Assert.assertEquals(expectedSize, out.buffer().limit());
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                StreamRequest decoded = StreamRequest.serializer.deserialize(in, version);

                Assert.assertEquals(orig.keyspace, decoded.keyspace);
                Util.assertRCEquals(orig.full, decoded.full);
                Util.assertRCEquals(orig.transientReplicas, decoded.transientReplicas);
                Assert.assertEquals(orig.columnFamilies, decoded.columnFamilies);
            }
        }
    }

    private static RangesAtEndpoint atEndpoint(Collection<Range<Token>> full, Collection<Range<Token>> trans)
    {
        RangesAtEndpoint.Builder builder = RangesAtEndpoint.builder(local);
        for (Range<Token> range : full)
            builder.add(new Replica(local, range, true));

        for (Range<Token> range : trans)
            builder.add(new Replica(local, range, false));

        return builder.build();
    }

    private static Range<Token> range(int l, int r)
    {
        return new Range<>(new ByteOrderedPartitioner.BytesToken(Integer.toString(l).getBytes()),
                           new ByteOrderedPartitioner.BytesToken(Integer.toString(r).getBytes()));
    }
}
