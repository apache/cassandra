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
package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CounterId;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

/**
 * Test that digest for pre-3.0 versions are properly computed (they match the value computed on pre-3.0 nodes).
 *
 * The concreted 'hard-coded' digests this file tests against have been generated on a 2.2 node using basically
 * the same test file but with 2 modifications:
 *   1. readAndDigest is modified to work on 2.2 (the actual modification is in the method as a comment)
 *   2. the assertions are replace by simple println() of the generated digest.
 *
 * Note that we only compare against 2.2 since digests should be fixed between version before 3.0 (this would be a bug
 * of previous version otherwise).
 */
public class DigestBackwardCompatibilityTest extends CQLTester
{
    private ByteBuffer readAndDigest(String partitionKey)
    {
        /*
         * In 2.2, this must be replaced by:
         *   ColumnFamily partition = getCurrentColumnFamilyStore().getColumnFamily(QueryFilter.getIdentityFilter(Util.dk(partitionKey), currentTable(), System.currentTimeMillis()));
         *   return ColumnFamily.digest(partition);
         */

        ReadCommand cmd = Util.cmd(getCurrentColumnFamilyStore(), partitionKey).build();
        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(cmd);
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredRowIterators.digest(cmd, partition.unfilteredIterator(), digest, MessagingService.VERSION_22);
        return ByteBuffer.wrap(digest.digest());
    }

    private void assertDigest(String expected, ByteBuffer actual)
    {
        String toTest = ByteBufferUtil.bytesToHex(actual);
        assertEquals(String.format("[digest from 2.2] %s != %s [digest from 3.0]", expected, toTest), expected, toTest);
    }

    @Test
    public void testCQLTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, v1 text, v2 int, PRIMARY KEY (k, t))");

        String key = "someKey";

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s(k, t, v1, v2) VALUES (?, ?, ?, ?) USING TIMESTAMP ? AND TTL ?", key, i, "v" + i, i, 1L, 200);

        // ColumnFamily(table_0 [0::false:0@1!200,0:v1:false:2@1!200,0:v2:false:4@1!200,1::false:0@1!200,1:v1:false:2@1!200,1:v2:false:4@1!200,2::false:0@1!200,2:v1:false:2@1!200,2:v2:false:4@1!200,3::false:0@1!200,3:v1:false:2@1!200,3:v2:false:4@1!200,4::false:0@1!200,4:v1:false:2@1!200,4:v2:false:4@1!200,5::false:0@1!200,5:v1:false:2@1!200,5:v2:false:4@1!200,6::false:0@1!200,6:v1:false:2@1!200,6:v2:false:4@1!200,7::false:0@1!200,7:v1:false:2@1!200,7:v2:false:4@1!200,8::false:0@1!200,8:v1:false:2@1!200,8:v2:false:4@1!200,9::false:0@1!200,9:v1:false:2@1!200,9:v2:false:4@1!200,])
        assertDigest("aa608035cf6574a97061b5c166b64939", readAndDigest(key));

        // This is a cell deletion
        execute("DELETE v1 FROM %s USING TIMESTAMP ? WHERE k = ? AND t = ?", 2L, key, 2);

        // This is a range tombstone
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND t = ?", 3L, key, 4);

        // This is a partition level deletion (but we use an older tombstone so it doesn't get rid of everything and keeps the test interesting)
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ?", 0L, key);

        // ColumnFamily(table_0 -{deletedAt=0, localDeletion=1441012270, ranges=[4:_-4:!, deletedAt=3, localDeletion=1441012270]}- [0::false:0@1!200,0:v1:false:2@1!200,0:v2:false:4@1!200,1::false:0@1!200,1:v1:false:2@1!200,1:v2:false:4@1!200,2::false:0@1!200,2:v1:true:4@2,2:v2:false:4@1!200,3::false:0@1!200,3:v1:false:2@1!200,3:v2:false:4@1!200,5::false:0@1!200,5:v1:false:2@1!200,5:v2:false:4@1!200,6::false:0@1!200,6:v1:false:2@1!200,6:v2:false:4@1!200,7::false:0@1!200,7:v1:false:2@1!200,7:v2:false:4@1!200,8::false:0@1!200,8:v1:false:2@1!200,8:v2:false:4@1!200,9::false:0@1!200,9:v1:false:2@1!200,9:v2:false:4@1!200,])
        assertDigest("b5f38d2dc7b917d221f98ab1641f82bf", readAndDigest(key));
    }

    @Test
    public void testCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, t int, v text, PRIMARY KEY (k, t)) WITH COMPACT STORAGE");

        String key = "someKey";

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s(k, t, v) VALUES (?, ?, ?) USING TIMESTAMP ? AND TTL ?", key, i, "v" + i, 1L, 200);

        assertDigest("44785ddd7c62c73287b448b6063645e5", readAndDigest(key));

        // This is a cell deletion
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ? AND t = ?", 2L, key, 2);

        // This is a partition level deletion (but we use an older tombstone so it doesn't get rid of everything and keeps the test interesting)
        execute("DELETE FROM %s USING TIMESTAMP ? WHERE k = ?", 0L, key);

        assertDigest("55d9bd6335276395d83b18eb17f9abe7", readAndDigest(key));
    }

    @Test
    public void testStaticCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v1 text, v2 int) WITH COMPACT STORAGE");

        String key = "someKey";
        execute("INSERT INTO %s(k, v1, v2) VALUES (?, ?, ?) USING TIMESTAMP ?", key, "v", 0, 1L);

        assertDigest("d2080f9f57d6edf92da1fdaaa76573d3", readAndDigest(key));
    }

    @Test
    public void testTableWithCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, m map<text, text>)");

        String key = "someKey";

        execute("INSERT INTO %s(k, m) VALUES (?, { 'foo' : 'value1', 'bar' : 'value2' }) USING TIMESTAMP ?", key, 1L);

        // ColumnFamily(table_2 -{deletedAt=-9223372036854775808, localDeletion=2147483647, ranges=[m:_-m:!, deletedAt=0, localDeletion=1441012271]}- [:false:0@1,m:626172:false:6@1,m:666f6f:false:6@1,])
        assertDigest("708f3fc8bc8149cc3513eef300bf0182", readAndDigest(key));

        // This is a collection range tombstone
        execute("DELETE m FROM %s USING TIMESTAMP ? WHERE k = ?", 2L, key);

        // ColumnFamily(table_2 -{deletedAt=-9223372036854775808, localDeletion=2147483647, ranges=[m:_-m:!, deletedAt=2, localDeletion=1441012271]}- [:false:0@1,])
        assertDigest("f39937fc3ed96956ef507e81717fa5cd", readAndDigest(key));
    }

    @Test
    public void testCounterTable() throws Throwable
    {
        /*
         * We can't use CQL to insert counters as both the timestamp and counter ID are automatically assigned and unpredictable.
         * So we need to built it ourselves in a way that is totally equivalent between 2.2 and 3.0 which makes the test a little
         * bit less readable. In any case, the code to generate the equivalent mutation on 2.2 is:
         * ColumnFamily cf = ArrayBackedSortedColumns.factory.create(getCurrentColumnFamilyStore().metadata);
         * ByteBuffer value = CounterContext.instance().createGlobal(CounterId.fromInt(1), 1L, 42L);
         * cf.addColumn(new BufferCounterCell(CellNames.simpleSparse(new ColumnIdentifier("c", true)) , value, 0L, Long.MIN_VALUE));
         * new Mutation(KEYSPACE, ByteBufferUtil.bytes(key), cf).applyUnsafe();
         *
         * Also note that we use COMPACT STORAGE only because it has no bearing on the test and was slightly easier in 2.2 to create
         * the mutation.
         */

        createTable("CREATE TABLE %s (k text PRIMARY KEY, c counter) WITH COMPACT STORAGE");

        String key = "someKey";

        CFMetaData metadata = getCurrentColumnFamilyStore().metadata;
        ColumnDefinition column = metadata.getColumnDefinition(ByteBufferUtil.bytes("c"));
        ByteBuffer value = CounterContext.instance().createGlobal(CounterId.fromInt(1), 1L, 42L);
        Row row = BTreeRow.singleCellRow(Clustering.STATIC_CLUSTERING, BufferCell.live(column, 0L, value));

        new Mutation(PartitionUpdate.singleRowUpdate(metadata, Util.dk(key), row)).applyUnsafe();

        assertDigest("3a5f7b48c320538b4cd2f829e05c6db3", readAndDigest(key));
    }
}
