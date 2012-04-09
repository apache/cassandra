/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.gms;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputBuffer;
import java.net.InetAddress;

import org.apache.cassandra.net.MessagingService;
import org.junit.Test;

public class GossipDigestTest
{
    @Test
    public void test() throws IOException
    {
        InetAddress endpoint = InetAddress.getByName("127.0.0.1");
        int generation = 0;
        int maxVersion = 123;
        GossipDigest expected = new GossipDigest(endpoint, generation, maxVersion);
        //make sure we get the same values out
        assertEquals(endpoint, expected.getEndpoint());
        assertEquals(generation, expected.getGeneration());
        assertEquals(maxVersion, expected.getMaxVersion());

        //test the serialization and equals
        DataOutputBuffer output = new DataOutputBuffer();
        GossipDigest.serializer.serialize(expected, output, MessagingService.current_version);

        ByteArrayInputStream input = new ByteArrayInputStream(output.getData(), 0, output.getLength());
        GossipDigest actual = GossipDigest.serializer.deserialize(new DataInputStream(input), MessagingService.current_version);
        assertEquals(0, expected.compareTo(actual));
    }
}
