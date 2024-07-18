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

import java.util.List;

import org.junit.Test;

import accord.local.RedundantBefore;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService.Version;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.net.MessagingService.Version.VERSION_51;

public class CommandStoreSerializersTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final List<Version> SUPPORTED_VERSIONS = VERSION_51.greaterThanOrEqual();

    @Test
    public void redundantBeforeEntry()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(Gens.random(), AccordGenerators.partitioner()).check((rs, partitioner) -> {
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            RedundantBefore.Entry entry = AccordGenerators.redundantBeforeEntry(partitioner).next(rs);
            for (Version version : SUPPORTED_VERSIONS)
                IVersionedSerializers.testSerde(buffer, CommandStoreSerializers.redundantBeforeEntry, entry, version.value);
        });
    }

    @Test
    public void redundantBefore()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().forAll(Gens.random(), AccordGenerators.partitioner()).check((rs, partitioner) -> {
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            // serializer doesn't support the empty set, so filter out
            RedundantBefore redundantBefore = AccordGenerators.redundantBefore(partitioner).filter(r -> r.size() != 0).next(rs);
            for (Version version : SUPPORTED_VERSIONS)
                IVersionedSerializers.testSerde(buffer, CommandStoreSerializers.redundantBefore, redundantBefore, version.value);
        });
    }

}