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
package org.apache.cassandra.index.sai.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

public class TokenCollisionTest extends SAITester
{
    @Test
    public void skinnyPartitionTest()
    {
        doSkinnyPartitionTest(10, 0);
    }

    @Test
    public void skinnyPartitionLastRowTest()
    {
        doSkinnyPartitionTest(49, 9);
    }

    private void doSkinnyPartitionTest(int v1Match, int v2Match)
    {
        createTable("CREATE TABLE %s (pk blob, v1 int, v2 int, PRIMARY KEY (pk))");
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        ByteBuffer prefix = ByteBufferUtil.bytes("key");
        int numRows = 100;
        int v1Count = 0;
        int v2Count = 0;
        List<Object[]> matchingPks = new ArrayList<>();
        for (int pkCount = 0; pkCount < numRows; pkCount++)
        {
            ByteBuffer pk = Util.generateMurmurCollision(prefix, (byte) (pkCount / 64), (byte) (pkCount % 64));
            if (v1Count == v1Match && v2Count == v2Match)
                matchingPks.add(row(pk, v1Count, v2Count));
            execute("INSERT INTO %s (pk, v1, v2) VALUES (?, ?, ?)", pk, v1Count++, v2Count++);
            if (v1Count == 50)
                v1Count = 0;
            if (v2Count == 10)
                v2Count = 0;
        }
        assertEquals(2, matchingPks.size());
        flush();

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE v1=" + v1Match + " AND v2=" + v2Match), matchingPks.get(0), matchingPks.get(1));
    }

    @Test
    public void widePartitionTest()
    {
        doWidePartitionTest(100, 10, 0);
    }

    @Test
    public void widePartitionLastRowTest()
    {
        // Reduce the number of rows so the last row occurs at the first clustering value
        doWidePartitionTest(97, 46, 6);
    }

    private void doWidePartitionTest(int numRows, int v1Match, int v2Match)
    {
        createTable("CREATE TABLE %s (pk blob, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v1) USING 'sai'");
        createIndex("CREATE INDEX ON %s(v2) USING 'sai'");

        ByteBuffer prefix = ByteBufferUtil.bytes("key");
        int pkCount = 0;
        int ckCount = 0;
        int v1Count = 0;
        int v2Count = 0;
        List<Object[]> matchingRows = new ArrayList<>();
        for (int i = 0; i < numRows; i++)
        {
            ByteBuffer pk = Util.generateMurmurCollision(prefix, (byte) (pkCount / 64), (byte) (pkCount % 64));
            if (v1Count == v1Match && v2Count == v2Match)
                matchingRows.add(row(pk, ckCount, v1Count, v2Count));
            execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)", pk, ckCount++, v1Count++, v2Count++);
            if (ckCount == 8)
            {
                ckCount = 0;
                pkCount++;
            }
            if (v1Count == 50)
                v1Count = 0;
            if (v2Count == 10)
                v2Count = 0;
        }
        assertEquals(2, matchingRows.size());
        flush();

        assertRowsIgnoringOrder(execute("SELECT * FROM %s WHERE v1=" + v1Match + " AND v2=" + v2Match), matchingRows.get(0), matchingRows.get(1));
    }
}
