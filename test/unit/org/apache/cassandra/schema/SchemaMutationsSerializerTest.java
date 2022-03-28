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

package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.Collection;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.assertj.core.api.Assertions;

public class SchemaMutationsSerializerTest extends CQLTester
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testSerDe() throws IOException
    {
        createTable("CREATE TABLE %s (id INT PRIMARY KEY, v INT)");
        Collection<Mutation> mutations = SchemaKeyspace.convertSchemaToMutations();
        DataOutputBuffer out = new DataOutputBuffer();
        SchemaMutationsSerializer.instance.serialize(mutations, out, MessagingService.current_version);
        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        Collection<Mutation> deserializedMutations = SchemaMutationsSerializer.instance.deserialize(in, MessagingService.current_version);
        DataOutputBuffer out2 = new DataOutputBuffer();
        SchemaMutationsSerializer.instance.serialize(deserializedMutations, out2, MessagingService.current_version);
        Assertions.assertThat(out2.toByteArray()).isEqualTo(out.toByteArray());
    }
}