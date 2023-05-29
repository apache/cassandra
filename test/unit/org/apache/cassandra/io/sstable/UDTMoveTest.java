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

package org.apache.cassandra.io.sstable;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.SequentialWriter;

import static org.assertj.core.api.Assertions.assertThat;

public class UDTMoveTest extends CQLTester
{
    @Test
    public void testMovingKeyspacdManually() throws Throwable
    {
        String table = "tab";
        String udt = "udt";
        String ks1 = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        String ks2 = createKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        schemaChange(String.format("CREATE TYPE %s.%s (a int, b int)", ks1, udt));
        schemaChange(String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, v udt)", ks1, table));
        schemaChange(String.format("CREATE TYPE %s.%s (a int, b int)", ks2, udt));
        schemaChange(String.format("CREATE TABLE %s.%s (id int PRIMARY KEY, v udt)", ks2, table));

        disableCompaction(ks1, table);
        disableCompaction(ks2, table);

        execute(String.format("INSERT INTO %s.%s (id, v) VALUES (1, {a: 1, b: 2})", ks1, table));
        execute(String.format("INSERT INTO %s.%s (id, v) VALUES (2, {a: 2, b: 3})", ks1, table));
        flush(ks1, table);

        // query data
        UntypedResultSet rows = execute(String.format("SELECT * FROM %s.%s", ks1, table));
        assertRows(rows, row(1, userType("a", 1, "b", 2)), row(2, userType("a", 2, "b", 3)));

        Set<SSTableReader> sstableReaders1 = getColumnFamilyStore(ks1, table).getLiveSSTables();
        Path sstablesLocation2 = getColumnFamilyStore(ks2, table).getDirectories().getDirectoryForNewSSTables().toPath();
        assertThat(sstableReaders1).hasSizeGreaterThan(0);

        for (SSTableReader reader : sstableReaders1)
        {
            for (Component component : SSTable.componentsFor(reader.descriptor))
            {
                Path sourcePath = reader.descriptor.pathFor(component);
                if (Files.exists(sourcePath))
                {
                    logger.info("Copying {} to {}", sourcePath, sstablesLocation2.resolve(sourcePath.getFileName()));

                    if (component.equals(Component.STATS))
                    {
                        Map<MetadataType, MetadataComponent> metadata = new HashMap<>(new MetadataSerializer().deserialize(reader.descriptor, EnumSet.allOf(MetadataType.class)));
                        SerializationHeader.Component serializationHeader = (SerializationHeader.Component) metadata.get(MetadataType.HEADER);
                        serializationHeader = serializationHeader.withMigratedKeyspaces(ImmutableMap.of(ks1, ks2));
                        metadata.put(MetadataType.HEADER, serializationHeader);

                        try (SequentialWriter out = new SequentialWriter(new File(sstablesLocation2.resolve(sourcePath.getFileName()))))
                        {
                            new MetadataSerializer().serialize(metadata, out, reader.descriptor.version);
                            out.sync();
                        }

                        assertThat(Files.exists(sstablesLocation2.resolve(sourcePath.getFileName()))).isTrue();
                        assertThat(Files.size(sstablesLocation2.resolve(sourcePath.getFileName()))).isGreaterThan(0);
                    }
                    else
                    {
                        Files.copy(sourcePath, sstablesLocation2.resolve(sourcePath.getFileName()));
                    }
                }
            }
        }

        getColumnFamilyStore(ks2, table).loadNewSSTables();
        rows = execute(String.format("SELECT * FROM %s.%s", ks2, table));
        assertRows(rows, row(1, userType("a", 1, "b", 2)), row(2, userType("a", 2, "b", 3)));

        execute(String.format("INSERT INTO %s.%s (id, v) VALUES (2, {a: 3, b: 2})", ks2, table));
        execute(String.format("INSERT INTO %s.%s (id, v) VALUES (3, {a: 3, b: 4})", ks2, table));
        rows = execute(String.format("SELECT * FROM %s.%s", ks2, table));
        assertRows(rows, row(1, userType("a", 1, "b", 2)), row(2, userType("a", 3, "b", 2)), row(3, userType("a", 3, "b", 4)));

        flush(ks2, table);
        rows = execute(String.format("SELECT * FROM %s.%s", ks2, table));
        assertRows(rows, row(1, userType("a", 1, "b", 2)), row(2, userType("a", 3, "b", 2)), row(3, userType("a", 3, "b", 4)));

        compact(ks2, table);
        rows = execute(String.format("SELECT * FROM %s.%s", ks2, table));
        assertRows(rows, row(1, userType("a", 1, "b", 2)), row(2, userType("a", 3, "b", 2)), row(3, userType("a", 3, "b", 4)));
    }
}