/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.index.sai.functional;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;

import static org.junit.Assert.assertEquals;

public class FlushingTest extends SAITester
{
    @Test
    public void testFlushingLargeStaleMemtableIndex() throws Throwable
    {
        createTable(CREATE_TABLE_TEMPLATE);
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        // BDKWriter#valueCount is updated when leaf values are written at BKDWriter#writeLeakBlock on every
        // BKDWriter#DEFAULT_MAX_POINTS_IN_LEAF_NODE (1024) number of points, see LUCENE-8765
        int overwrites = NumericIndexWriter.MAX_POINTS_IN_LEAF_NODE + 1;
        for (int j = 0; j < overwrites; j++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES ('1', ?)", j);
        }

        flush();

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1>=0");
        assertEquals(1, rows.all().size());
    }

    @Test
    public void testFlushingOverwriteDelete() throws Throwable
    {
        String table = "flush_overwrite_delete";
        createTable(CREATE_TABLE_TEMPLATE);
        String v1IndexName = createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1"));

        int sstables = 3;
        for (int j = 0; j < sstables; j++)
        {
            execute("INSERT INTO %s (id1, v1) VALUES (?, 1)", Integer.toString(j));
            execute("DELETE FROM %s WHERE id1 = ?", Integer.toString(j));
            flush();
        }

        ResultSet rows = executeNet("SELECT id1 FROM %s WHERE v1 >= 0");
        assertEquals(0, rows.all().size());
        verifyIndexFiles(sstables, 0, 0, sstables);
        verifySSTableIndexes(v1IndexName, sstables, 0);

        compact();
        waitForAssert(() -> verifyIndexFiles(1, 0, 0, 1));

        rows = executeNet("SELECT id1 FROM %s WHERE v1 >= 0");
        assertEquals(0, rows.all().size());
        verifySSTableIndexes(v1IndexName, 1, 0);
    }
}
