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

package org.apache.cassandra.tcm.membership;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.junit.Assert.assertEquals;

public class NodeVersionTest
{
    @Test
    public void futureNodeVersionTest() throws IOException
    {
        ByteBuffer bb;
        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            NodeVersion.Serializer.serializeHelper(dob, "8.0.0", NodeVersion.CURRENT.serializationVersion + 1);
            bb = dob.asNewBuffer();
        }

        try (DataInputBuffer in = new DataInputBuffer(bb, false))
        {
            NodeVersion n = NodeVersion.serializer.deserialize(in, NodeVersion.CURRENT.serializationVersion());
            assertEquals(n.serializationVersion().asInt(), Version.UNKNOWN.asInt());
        }
    }
}
