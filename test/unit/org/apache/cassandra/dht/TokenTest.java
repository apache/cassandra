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

package org.apache.cassandra.dht;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.IVersionedSerializers;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;

import static accord.utils.Property.qt;

public class TokenTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void serde()
    {
        DataOutputBuffer output = new DataOutputBuffer();
        qt().check(rs -> {
            IPartitioner partitioner = AccordGenerators.partitioner().next(rs);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Token token = AccordGenerators.fromQT(CassandraGenerators.token(partitioner)).next(rs);
            for (MessagingService.Version version : MessagingService.Version.values())
                IVersionedSerializers.testSerde(output, Token.compactSerializer, token, version.value);
        });
    }
}