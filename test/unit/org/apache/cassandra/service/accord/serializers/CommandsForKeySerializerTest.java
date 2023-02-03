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

import org.junit.BeforeClass;
import org.junit.Test;

import accord.impl.CommandsForKey;
import accord.primitives.TxnId;
import accord.utils.Gens;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AccordGenerators;

import static accord.utils.Property.qt;
import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;
import static org.assertj.core.api.Assertions.assertThat;

public class CommandsForKeySerializerTest
{
    @BeforeClass
    public static void beforeClass() throws Throwable
    {
        // need to create the accord test table as generating random txn is not currently supported
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("ks", KeyspaceParams.simple(1),
                                    parse("CREATE TABLE tbl (k int, c int, v int, primary key (k, c))", "ks"));
        StorageService.instance.initServer();
    }

    @Test
    public void serdeDeps()
    {
        DataOutputBuffer buffer = new DataOutputBuffer();
        int version = MessagingService.Version.VERSION_40.value;
        qt().forAll(Gens.lists(AccordGenerators.ids()).ofSizeBetween(0, 10)).check(ids -> {
            buffer.clear();

            long expectedSize = CommandsForKeySerializer.depsIdSerializer.serializedSize(ids, version);

            CommandsForKeySerializer.depsIdSerializer.serialize(ids, buffer, version);
            assertThat(buffer.position()).isEqualTo(expectedSize);
            try (DataInputBuffer in = new DataInputBuffer(buffer.unsafeGetBufferAndFlip(), false))
            {
                List<TxnId> read = CommandsForKeySerializer.depsIdSerializer.deserialize(in, version);
                assertThat(read).isEqualTo(ids);
            }
        });
    }

    @Test
    public void serde()
    {
        CommandsForKey.CommandLoader<ByteBuffer> loader = CommandsForKeySerializer.loader;
        qt().forAll(AccordGenerators.commands()).check(cmd -> {
            ByteBuffer bb = loader.saveForCFK(cmd);
            int size = bb.remaining();

            assertThat(loader.txnId(bb)).isEqualTo(cmd.txnId());
            assertThat(bb.remaining()).describedAs("ByteBuffer was mutated").isEqualTo(size);

            assertThat(loader.executeAt(bb)).isEqualTo(cmd.executeAt());
            assertThat(bb.remaining()).describedAs("ByteBuffer was mutated").isEqualTo(size);

            assertThat(loader.saveStatus(bb)).isEqualTo(cmd.saveStatus());
            assertThat(bb.remaining()).describedAs("ByteBuffer was mutated").isEqualTo(size);

            assertThat(loader.depsIds(bb)).isEqualTo(cmd.partialDeps() == null ? null : cmd.partialDeps().txnIds());
            assertThat(bb.remaining()).describedAs("ByteBuffer was mutated").isEqualTo(size);
        });
    }
}