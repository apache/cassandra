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
package org.apache.cassandra.cql3;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import static org.junit.Assert.assertEquals;

public class SSTableMetadataTrackingTest extends CQLTester
{
    @Test
    public void baseCheck() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("INSERT INTO %s (a,b,c) VALUES (1,1,'1') using timestamp 9999");
        cfs.forceBlockingFlush();
        StatsMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime);
        cfs.forceMajorCompaction();
        metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime);
    }

    @Test
    public void testMinMaxtimestampRange() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("INSERT INTO %s (a,b,c) VALUES (1,1,'1') using timestamp 10000");
        execute("DELETE FROM %s USING TIMESTAMP 9999 WHERE a = 1 and b = 1");
        cfs.forceBlockingFlush();
        StatsMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
    }

    @Test
    public void testMinMaxtimestampRow() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("INSERT INTO %s (a,b,c) VALUES (1,1,'1') using timestamp 10000");
        execute("DELETE FROM %s USING TIMESTAMP 9999 WHERE a = 1");
        cfs.forceBlockingFlush();
        StatsMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(10000, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime, 5);
    }


    @Test
    public void testTrackMetadata_rangeTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b)) WITH gc_grace_seconds = 10000");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("DELETE FROM %s USING TIMESTAMP 9999 WHERE a = 1 and b = 1");
        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        StatsMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(System.currentTimeMillis()/1000, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        StatsMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }

    @Test
    public void testTrackMetadata_rowTombstone() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("DELETE FROM %s USING TIMESTAMP 9999 WHERE a = 1");

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        StatsMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(System.currentTimeMillis()/1000, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        StatsMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }

    @Test
    public void testTrackMetadata_rowMarker() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, PRIMARY KEY (a))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("INSERT INTO %s (a) VALUES (1) USING TIMESTAMP 9999");

        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        StatsMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(Integer.MAX_VALUE, metadata.maxLocalDeletionTime);
        cfs.forceMajorCompaction();
        StatsMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }
    @Test
    public void testTrackMetadata_rowMarkerDelete() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, PRIMARY KEY (a))");
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(currentTable());
        execute("DELETE FROM %s USING TIMESTAMP 9999 WHERE a=1");
        cfs.forceBlockingFlush();
        assertEquals(1, cfs.getSSTables().size());
        StatsMetadata metadata = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(9999, metadata.minTimestamp);
        assertEquals(9999, metadata.maxTimestamp);
        assertEquals(System.currentTimeMillis()/1000, metadata.maxLocalDeletionTime, 5);
        cfs.forceMajorCompaction();
        StatsMetadata metadata2 = cfs.getSSTables().iterator().next().getSSTableMetadata();
        assertEquals(metadata.maxLocalDeletionTime, metadata2.maxLocalDeletionTime);
        assertEquals(metadata.minTimestamp, metadata2.minTimestamp);
        assertEquals(metadata.maxTimestamp, metadata2.maxTimestamp);
    }
}
