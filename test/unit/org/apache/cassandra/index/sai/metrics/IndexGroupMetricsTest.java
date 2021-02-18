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

import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.InvertedIndexSearcher;
import org.apache.cassandra.index.sai.disk.KDTreeIndexSearcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;


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

        // no open files
        assertEquals(0, getOpenIndexFiles());
        assertEquals(0, getDiskUsage());

        int sstables = 10;
        for (int i = 0; i < sstables; i++)
        {
            execute("INSERT INTO %s (id1, v1, v2) VALUES ('0', 0, '0')");
            flush();
        }

        // with 10 sstable
        int indexopenFileCountWithOnlyNumeric = getOpenIndexFiles();
        assertEquals(sstables * (SSTableContext.openFilesPerSSTable() + KDTreeIndexSearcher.openPerIndexFiles()), indexopenFileCountWithOnlyNumeric);

        long diskUsageWithOnlyNumeric = getDiskUsage();
        assertNotEquals(0, diskUsageWithOnlyNumeric);

        // create second index
        String v2IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2"));
        waitForIndexQueryable();

        // same number of sstables, but more string index files.
        int stringIndexOpenFileCount = sstables * InvertedIndexSearcher.openPerIndexFiles();
        assertEquals(indexopenFileCountWithOnlyNumeric, getOpenIndexFiles() - stringIndexOpenFileCount);

        // Index Group disk usage doesn't change with more indexes
        long diskUsageWithBothIndexes = getDiskUsage();
        assertEquals(diskUsageWithBothIndexes, diskUsageWithOnlyNumeric);

        // compaction should reduce open files
        compact();

        long perSSTableFileDiskUsage = getDiskUsage();
        assertEquals(SSTableContext.openFilesPerSSTable() + KDTreeIndexSearcher.openPerIndexFiles() + InvertedIndexSearcher.openPerIndexFiles(),
                     getOpenIndexFiles());

        // drop string index, reduce open string index files, per-sstable file disk usage remains the same
        dropIndex("DROP INDEX %s." + v2IndexName);
        assertEquals(SSTableContext.openFilesPerSSTable() + KDTreeIndexSearcher.openPerIndexFiles(), getOpenIndexFiles());
        assertEquals(perSSTableFileDiskUsage, getDiskUsage());

        // drop last index, no open index files
        dropIndex("DROP INDEX %s." + v1IndexName);
        assertEquals(0, getOpenIndexFiles());
        assertEquals(0, getDiskUsage());
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
