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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializers;
import org.apache.cassandra.tcm.serialization.Version;

public class AccordMarkStaleTest
{
    @Test
    public void shouldSerializeEmpty() throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        AsymmetricMetadataSerializers.testSerde(buffer, AccordMarkStale.serializer, new AccordMarkStale(Collections.emptySet()), Version.MIN_ACCORD_VERSION);
    }

    @Test
    public void shouldSerializeSingleton() throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        AccordMarkStale markStale = new AccordMarkStale(Collections.singleton(NodeId.fromString("1")));
        AsymmetricMetadataSerializers.testSerde(buffer, AccordMarkStale.serializer, markStale, Version.MIN_ACCORD_VERSION);
    }

    @Test
    public void shouldSerializeMulti() throws IOException
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        AccordMarkStale markStale = new AccordMarkStale(ImmutableSet.of(NodeId.fromString("1"), NodeId.fromString("2")));
        AsymmetricMetadataSerializers.testSerde(buffer, AccordMarkStale.serializer, markStale, Version.MIN_ACCORD_VERSION);
    }
}
