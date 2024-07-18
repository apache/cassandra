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

import accord.primitives.Deps;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaProvider;
import org.apache.cassandra.utils.AccordGenerators;
import org.mockito.Mockito;

import static accord.utils.Property.qt;
import static org.apache.cassandra.net.MessagingService.Version.VERSION_51;

public class DepsSerializerTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final List<MessagingService.Version> SUPPORTED_VERSIONS = VERSION_51.greaterThanOrEqual();

    @Test
    public void serde()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        qt().withSeed(-4368731546033726179L).check(rs -> {
            IPartitioner partitioner = AccordGenerators.partitioner().next(rs);
            Schema.instance = Mockito.mock(SchemaProvider.class);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Mockito.when(Schema.instance.getExistingTablePartitioner(Mockito.any())).thenReturn(partitioner);
            Deps deps = AccordGenerators.depsGen(partitioner).next(rs);
            for (MessagingService.Version version : SUPPORTED_VERSIONS)
                IVersionedSerializers.testSerde(buffer, DepsSerializer.deps, deps, version.value);
        });
    }
}