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

package org.apache.cassandra.db;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Objects;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CorruptedSSTableQueryTest
{
    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
    }

    @Test
    public void testReadCorruptedTable() throws Exception
    {
        String keyspace = "harry";
        String tableName = "table_1";

        SchemaLoader.createKeyspace(keyspace,
                KeyspaceParams.simple(2),
                CreateTableStatement.parse(String.format("CREATE TABLE %s.%s (" +
                                "    pk0003 text," +
                                "    pk0004 ascii," +
                                "    pk0005 float," +
                                "    ck0002 int," +
                                "    ck0003 text," +
                                "    ck0004 ascii," +
                                "    regular0003 float," +
                                "    regular0004 ascii," +
                                "    regular0005 text," +
                                "    regular0006 float," +
                                "    regular0007 text," +
                                "    PRIMARY KEY ((pk0003, pk0004, pk0005), ck0002, ck0003, ck0004)" +
                                ") WITH CLUSTERING ORDER BY (ck0002 ASC, ck0003 DESC, ck0004 DESC)", keyspace, tableName), keyspace)
                        .partitioner(Murmur3Partitioner.instance));

        CompactionManager.instance.disableAutoCompaction();

        Keyspace k = Keyspace.open(keyspace);
        ColumnFamilyStore cfs = k.getColumnFamilyStore(tableName);

        java.io.File testDataDir = new java.io.File("test/data/cassandra-18932/data1/harry/table_1");
        for (org.apache.cassandra.io.util.File cfsDir : cfs.getDirectories().getCFDirectories())
            FileUtils.copyDirectory(testDataDir, cfsDir.toJavaIOFile());

        Descriptor descriptor = Descriptor.fromFileWithComponent(new File("test/data/cassandra-18932/data1/harry/table_1/nc-5-big-Data.db"), false).left;
        SSTableReader sst = SSTableReader.openNoValidation(null, descriptor, TableMetadataRef.forOfflineTools(cfs.metadata()));
        cfs.getTracker().addInitialSSTables(Collections.singletonList(sst));

        UntypedResultSet rs = executeInternal("SELECT * FROM harry.table_1 WHERE pk0003 = ? " +
                        "AND pk0004 = 'ZinzDdUuABgDknItABgDknItABgDknItABgDknItABgDknItzHqchghqCXLhVYKM22215251' " +
                        "AND pk0005 = 3.2758E-41 AND ck0002 = -1110871748 " +
                        "AND ck0003 = 'ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyaFfLoPrEzlMDvLfXY18918213101196160' " +
                        "AND ck0004 < 'ZYFiYEUkzcKOhdyazcKOhdyazcKOhdyazcKOhdyazcKOhdyachTAyMjmsZMUPCzi23819065184175';",
                "ZinzDdUuABgDknItABgDknItABgDknItXEFrgBnOmPmPylWrwXHqjBHgeQrGfnZd1124124583");
        assertNotNull(rs);
        assertTrue(rs.isEmpty());
    }
}
