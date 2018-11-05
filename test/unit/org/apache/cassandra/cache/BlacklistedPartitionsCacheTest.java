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

package org.apache.cassandra.cache;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BlacklistedPartitionsCacheTest extends CQLTester
{
    @BeforeClass
    public static void setUp() throws Throwable
    {
        // since system_distributed keyspace is required
        requireNetwork();
    }

    @Test
    public void testCacheRefresh() throws Throwable
    {
        // invalid blacklisted partition since ks/cf do not exist
        execute(String.format("INSERT INTO %s.%s (keyspace_name, columnfamily_name, partition_key) VALUES (?, ?, ?)", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS), "testks", "testcf", "samplekey");
        BlacklistedPartitionCache.instance.refreshCache();
        Assert.assertTrue(BlacklistedPartitionCache.instance.size() == 0);

        // create a test table
        createTable("CREATE TABLE %s (id int PRIMARY KEY, v1 text, v2 text)");
        // valid blacklisted partition
        execute(String.format("INSERT INTO %s.%s (keyspace_name, columnfamily_name, partition_key) VALUES (?, ?, ?)", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS), KEYSPACE, currentTable(), "123");
        BlacklistedPartitionCache.instance.refreshCache();
        Assert.assertTrue(BlacklistedPartitionCache.instance.size() == 1);
    }

    @Test(expected = InvalidQueryException.class)
    public void testBlacklistingPartitionInSimpleTable() throws Throwable
    {
        // create a test table
        String table1 = createAndPopulateTable();

        // blacklist key2 from table1
        execute(String.format("INSERT INTO %s.%s (keyspace_name, columnfamily_name, partition_key) VALUES (?, ?, ?)", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS), KEYSPACE, table1, "2");

        // refresh blacklist cache
        BlacklistedPartitionCache.instance.refreshCache();

        Session session = sessionNet();

        // querying nonblacklisted partition on table1 should be successful
        PreparedStatement pstmt = session.prepare("SELECT id, v1, v2 FROM " + KEYSPACE + '.' + table1 + " WHERE id = ?");
        ResultSet rs = session.execute(pstmt.bind(1));
        Assert.assertEquals(1, rs.all().size());

        // querying blacklisted partition on table1 should throw an InvalidQueryException
        pstmt = session.prepare("SELECT id, v1, v2 FROM " + KEYSPACE + '.' + table1 + " WHERE id = ?");
        session.execute(pstmt.bind(2));
    }

    @Test(expected = InvalidQueryException.class)
    public void testBlacklistingPartitionInSimpleTableAndUsingINClause() throws Throwable
    {
        // create a test table
        String table1 = createAndPopulateTable();

        // blacklist key 2 from table1
        execute(String.format("INSERT INTO %s.%s (keyspace_name, columnfamily_name, partition_key) VALUES (?, ?, ?)", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS), KEYSPACE, table1, "2");

        // refresh blacklist cache
        BlacklistedPartitionCache.instance.refreshCache();

        Session session = sessionNet();

        // querying blacklisted partition on table1 should throw an InvalidQueryException
        PreparedStatement pstmt = session.prepare("SELECT id, v1, v2 FROM " + KEYSPACE + '.' + table1 + " WHERE id IN (?,?)");
        session.execute(pstmt.bind(1, 2));
    }

    @Test
    public void testQueryingBlacklistedPartitionUnrelatedTable() throws Throwable
    {
        // create a test table
        String table1 = createAndPopulateTable();

        // create another test table
        String table2 = createAndPopulateTable();

        // refresh blacklist cache
        BlacklistedPartitionCache.instance.refreshCache();

        Session session = sessionNet();

        // querying blacklisted partition on a different table should not have any impact on the query
        PreparedStatement pstmt = session.prepare("SELECT id, v1, v2 FROM " + KEYSPACE + '.' + table2 + " WHERE id = ?");
        ResultSet rs = session.execute(pstmt.bind(2));
        Assert.assertEquals(1, rs.all().size());

        // querying blacklisted partition on the same table but on a different partition key should not have any impact on the query
        pstmt = session.prepare("SELECT id, v1, v2 FROM " + KEYSPACE + '.' + table1 + " WHERE id = ?");
        rs = session.execute(pstmt.bind(1));
        Assert.assertEquals(1, rs.all().size());
    }

    @Test(expected = InvalidQueryException.class)
    public void testBlacklistingPartitionWithCompositeKey() throws Throwable
    {
        // create a test table
        createTable("CREATE TABLE %s (key1 blob, key2 text, key3 int, cc text, value text, PRIMARY KEY ((key1,key2,key3),cc))");
        execute(String.format("INSERT INTO %s.%s (key1 , key2 , key3 , cc , value) VALUES (?, ?, ?, ?, ?)", CQLTester.KEYSPACE, currentTable()), 0x12345F, "k12", 1, "cc1", "v1");
        execute(String.format("INSERT INTO %s.%s (key1 , key2 , key3 , cc , value) VALUES (?, ?, ?, ?, ?)", CQLTester.KEYSPACE, currentTable()), 0x12346F, "k12", 1, "cc1", "v1");

        // blacklist partition
        execute(String.format("INSERT INTO %s.%s (keyspace_name, columnfamily_name, partition_key) VALUES (?, ?, ?)", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS), KEYSPACE, currentTable(), "12345F:k12:1");

        // refresh blacklist cache
        BlacklistedPartitionCache.instance.refreshCache();

        Session session = sessionNet();

        // querying blacklisted partition on the table should throw an InvalidQueryException
        PreparedStatement pstmt = session.prepare("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE key1 = ? AND key2 = ? AND key3 = ? AND cc = ?");
        session.execute(pstmt.bind(ByteBufferUtil.hexToBytes("12345F"), "k12", 1, "cc1"));
    }

    @Test(expected = InvalidQueryException.class)
    public void testBlacklistingPartitionWithColonCharacter() throws Throwable
    {
        // create a test table
        createTable("CREATE TABLE %s (key1 blob, key2 text, key3 int, cc text, value text, PRIMARY KEY ((key1,key2,key3),cc))");
        execute(String.format("INSERT INTO %s.%s (key1 , key2 , key3 , cc , value) VALUES (?, ?, ?, ?, ?)", CQLTester.KEYSPACE, currentTable()), 0x12345F, "k12:colon", 1, "cc1", "v1");

        // blacklist partition
        execute(String.format("INSERT INTO %s.%s (keyspace_name, columnfamily_name, partition_key) VALUES (?, ?, ?)", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS), KEYSPACE, currentTable(), "12345F:k12\\:colon:1");

        // refresh blacklist cache
        BlacklistedPartitionCache.instance.refreshCache();

        Session session = sessionNet();

        // querying blacklisted partition on the table should throw an InvalidQueryException
        PreparedStatement pstmt = session.prepare("SELECT * FROM " + KEYSPACE + '.' + currentTable() + " WHERE key1 = ? AND key2 = ? AND key3 = ? AND cc = ?");
        session.execute(pstmt.bind(ByteBufferUtil.hexToBytes("12345F"), "k12:colon", 1, "cc1"));
    }

    @Test
    public void testFailureReadingBlacklistedPartitions() throws Throwable
    {
        // create a test table
        String table1 = createAndPopulateTable();

        // drop blacklisted partitions table
        execute(String.format("DROP TABLE %s.%s", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS));

        // refresh blacklist cache
        BlacklistedPartitionCache.instance.refreshCache();

        Session session = sessionNet();

        // querying nonblacklisted partition on table1 should be successful
        PreparedStatement pstmt = session.prepare("SELECT id, v1, v2 FROM " + KEYSPACE + '.' + table1 + " WHERE id = ?");
        ResultSet rs = session.execute(pstmt.bind(1));
        Assert.assertEquals(1, rs.all().size());

        // recreate the dropped blacklisted partitions table
        String createBlacklistedPartitionsStmt = String.format("CREATE TABLE %s.%s ("
                                                               + "keyspace_name text,"
                                                               + "columnfamily_name text,"
                                                               + "partition_key text,"
                                                               + "PRIMARY KEY ((keyspace_name, columnfamily_name), partition_key))", SchemaConstants.DISTRIBUTED_KEYSPACE_NAME, SystemDistributedKeyspace.BLACKLISTED_PARTITIONS);
        execute(createBlacklistedPartitionsStmt);
    }

    private String createAndPopulateTable() throws Throwable
    {
        String table = createTable("CREATE TABLE %s (id int PRIMARY KEY, v1 text, v2 text)");
        execute(String.format("INSERT INTO %s.%s (id, v1, v2) VALUES (?, ?, ?)", CQLTester.KEYSPACE, table), 1, "v11", "v21");
        execute(String.format("INSERT INTO %s.%s (id, v1, v2) VALUES (?, ?, ?)", CQLTester.KEYSPACE, table), 2, "v12", "v22");
        return table;
    }
}