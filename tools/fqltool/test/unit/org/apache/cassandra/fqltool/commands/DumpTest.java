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

package org.apache.cassandra.fqltool.commands;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.junit.Assert;
import org.junit.Test;

import net.openhft.chronicle.wire.ValueIn;
import net.openhft.chronicle.wire.WireIn;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.fql.FullQueryLogger;
import org.apache.cassandra.transport.ProtocolVersion;
import org.mockito.Mockito;

public class DumpTest
{
    @Test
    public void testDumpQueryNullValues()
    {
        String keyspace = "ks1";

        List<ByteBuffer> values = Arrays.asList(ByteBuffer.wrap(new byte[]{ 1 }), null);

        QueryOptions queryOptions = QueryOptions.create(
        ConsistencyLevel.LOCAL_QUORUM,
        values,
        true,
        1,
        null,
        null,
        ProtocolVersion.CURRENT,
        keyspace
        );

        ValueIn mockValueIn = Mockito.mock(ValueIn.class);
        Mockito.when(mockValueIn.text()).thenReturn("INSERT INTO ks1.t1 (k, v) VALUES (?,?)");

        WireIn mockWireIn = Mockito.mock(WireIn.class);
        Mockito.when(mockWireIn.read(FullQueryLogger.QUERY)).thenReturn(mockValueIn);

        StringBuilder sb = new StringBuilder();
        Dump.dumpQuery(queryOptions, mockWireIn, sb);

        String[] lines = sb.toString().split(System.lineSeparator());
        boolean valuesStarted = false;
        int count = 0;
        int nullcount = 0;
        for (String line : lines)
        {
            if (line.startsWith("Values:"))
            {
                valuesStarted = true;
                continue;
            }
            if (valuesStarted)
            {
                if ("-----".equals(line))
                {
                    continue;
                }
                if (null == values.get(count++))
                {
                    nullcount++;
                    Assert.assertEquals("null", line);
                }
            }
        }

        Assert.assertEquals(values.stream().filter(Objects::isNull).count(), nullcount);
    }

    @Test
    public void testDumpQueryValuesShouldHaveSeperator()
    {
        String keyspace = "ks1";
        int value = 1;
        List<ByteBuffer> values = Arrays.asList(ByteBuffer.wrap(new byte[]{ (byte) value }),
                                                ByteBuffer.wrap(new byte[]{ (byte) value }),
                                                ByteBuffer.wrap(new byte[]{ (byte) value }));

        QueryOptions queryOptions = QueryOptions.create(
        ConsistencyLevel.LOCAL_QUORUM,
        values,
        true,
        1,
        null,
        null,
        ProtocolVersion.CURRENT,
        keyspace
        );

        ValueIn mockValueIn = Mockito.mock(ValueIn.class);
        Mockito.when(mockValueIn.text()).thenReturn("INSERT INTO ks1.t1 (k, v1, v2) VALUES (?, ?, ?)");

        WireIn mockWireIn = Mockito.mock(WireIn.class);
        Mockito.when(mockWireIn.read(FullQueryLogger.QUERY)).thenReturn(mockValueIn);

        StringBuilder sb = new StringBuilder();
        Dump.dumpQuery(queryOptions, mockWireIn, sb);

        String[] lines = sb.toString().split(System.lineSeparator());
        boolean valuesStarted = false;
        int valueCount = 0;
        int separatorCount = 0;
        for (String line : lines)
        {
            if (!valuesStarted && line.startsWith("Values:"))
            {
                valuesStarted = true;
                continue;
            }
            if (valuesStarted)
            {
                valueCount++;
                if (valueCount % 2 == 0)
                {
                    Assert.assertEquals("-----", line);
                    separatorCount++;
                }
            }
        }
        Assert.assertEquals(values.size(), separatorCount);
    }
}
