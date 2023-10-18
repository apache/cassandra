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
package org.apache.cassandra.index.sai.metrics;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class IndexGroupMetricsTest extends AbstractMetricsTest
{
    @Before
    public void setup() throws Exception
    {
        requireNetwork();

        startJMXServer();

        createMBeanServerConnection();
    }

    @Test
    public void verifyIndexGroupMetrics() throws Throwable
    {
        // create first index
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));
        IndexContext v1IndexContext = createIndexContext(v1IndexName, UTF8Type.instance);

        // no open files
        assertEquals(0, getOpenIndexFiles());
        assertEquals(0, getDiskUsage());

        int sstables = 10;
        for (int i = 0; i < sstables; i++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES ('0', 0)");
            flush();
        }

        // with 10 sstable
        int indexopenFileCountWithOnlyNumeric = getOpenIndexFiles();
        assertEquals(sstables * (Version.LATEST.onDiskFormat().openFilesPerSSTableIndex(false) +
                                 Version.LATEST.onDiskFormat().openFilesPerColumnIndex(v1IndexContext)),
                     indexopenFileCountWithOnlyNumeric);

        long diskUsageWithOnlyNumeric = getDiskUsage();
        assertNotEquals(0, diskUsageWithOnlyNumeric);

        // compaction should reduce open files
        compact();

        assertEquals(Version.LATEST.onDiskFormat().openFilesPerSSTableIndex(false) +
                     Version.LATEST.onDiskFormat().openFilesPerColumnIndex(v1IndexContext),
                     getOpenIndexFiles());

        // drop last index, no open index files
        dropIndex("DROP INDEX %s." + v1IndexName);
        assertNull(getCurrentIndexGroup());
    }

    protected int getOpenIndexFiles()
    {
        return (int) getMetricValue(objectNameNoIndex("OpenIndexFiles", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }

    protected long getDiskUsage()
    {
        return (long) getMetricValue(objectNameNoIndex("DiskUsedBytes", KEYSPACE, currentTable(), "IndexGroupMetrics"));
    }
}
