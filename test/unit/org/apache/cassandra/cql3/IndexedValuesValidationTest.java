/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.fail;

public class IndexedValuesValidationTest extends CQLTester
{
    private static final int TOO_BIG = 1024 * 65;
    // CASSANDRA-8280/8081
    // reject updates with indexed values where value > 64k
    @Test
    public void testIndexOnCompositeValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(c)");
        failInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnClusteringColumnInsertPartitionKeyAndClusteringsOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a blob, b blob, c blob, d int, PRIMARY KEY (a, b, c))");
        createIndex("CREATE INDEX ON %s(b)");

        // CompositeIndexOnClusteringKey creates index entries composed of the
        // PK plus all of the non-indexed clustering columns from the primary row
        // so we should reject where len(a) + len(c) > 65560 as this will form the
        // total clustering in the index table
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(10);
        ByteBuffer c = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT - 99);

        failInsert("INSERT INTO %s (a, b, c, d) VALUES (?, ?, ?, 0)", a, b, c);
    }

    @Test
    public void testCompactTableWithValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b blob, PRIMARY KEY (a)) WITH COMPACT STORAGE");
        createIndex("CREATE INDEX ON %s(b)");
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnCollectionValueInsertPartitionKeyAndCollectionKeyOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a blob , b map<blob, int>, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(b)");

        // A collection key > 64k by itself will be rejected from
        // the primary table.
        // To test index validation we need to ensure that
        // len(b) < 64k, but len(a) + len(b) > 64k as that will
        // form the clustering in the index table
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT - 100);

        failInsert("UPDATE %s SET b[?] = 0 WHERE a = ?", b, a);
    }

    @Test
    public void testIndexOnCollectionKeyInsertPartitionKeyAndClusteringOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a blob, b blob, c map<blob, int>, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(KEYS(c))");

        // Basically the same as the case with non-collection clustering
        // CompositeIndexOnCollectionKeyy creates index entries composed of the
        // PK plus all of the clustering columns from the primary row, except the
        // collection element - which becomes the partition key in the index table
        ByteBuffer a = ByteBuffer.allocate(100);
        ByteBuffer b = ByteBuffer.allocate(FBUtilities.MAX_UNSIGNED_SHORT - 100);
        ByteBuffer c = ByteBuffer.allocate(10);

        failInsert("UPDATE %s SET c[?] = 0 WHERE a = ? and b = ?", c, a, b);
    }

    @Test
    public void testIndexOnPartitionKeyInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY ((a, b)))");
        createIndex("CREATE INDEX ON %s(a)");
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnClusteringColumnInsertValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b int, c blob, PRIMARY KEY (a, b))");
        createIndex("CREATE INDEX ON %s(b)");
        succeedInsert("INSERT INTO %s (a, b, c) VALUES (0, 0, ?)", ByteBuffer.allocate(TOO_BIG));
    }

    @Test
    public void testIndexOnFullCollectionEntryInsertCollectionValueOver64k() throws Throwable
    {
        createTable("CREATE TABLE %s(a int, b frozen<map<int, blob>>, PRIMARY KEY (a))");
        createIndex("CREATE INDEX ON %s(full(b))");
        Map<Integer, ByteBuffer> map = new HashMap();
        map.put(0, ByteBuffer.allocate(1024 * 65));
        failInsert("INSERT INTO %s (a, b) VALUES (0, ?)", map);
    }

    public void failInsert(String insertCQL, Object...args) throws Throwable
    {
        try
        {
            execute(insertCQL, args);
            fail("Expected statement to fail validation");
        }
        catch (Exception e)
        {
            // as expected
        }
    }

    public void succeedInsert(String insertCQL, Object...args) throws Throwable
    {
        execute(insertCQL, args);
        flush();
    }
}
