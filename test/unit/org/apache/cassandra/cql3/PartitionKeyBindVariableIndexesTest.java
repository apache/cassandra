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

import static org.junit.Assert.assertArrayEquals;

/**
 * CASSANDRA-21740: {@link CQLStatement#getPartitionKeyBindVariableIndexes()} must only report bind markers
 * from operators that restrict a partition key column to a value it must equal (EQ, single-value IN), since
 * those are the only bind markers safe for a token-aware client to use for routing. Operators whose bound
 * value excludes a value, bounds a range, or matches a pattern (NOT IN, !=, slices, LIKE, ...) must not be
 * reported, since the bound value does not identify the partition being queried.
 */
public class PartitionKeyBindVariableIndexesTest extends CQLTester
{
    @Test
    public void testEqAndInAreReported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertPartitionKeyBindIndexes(new short[]{ 0 }, "SELECT * FROM %s WHERE k = ?");
        assertPartitionKeyBindIndexes(new short[]{ 0 }, "SELECT * FROM %s WHERE k IN (?)");
    }

    @Test
    public void testNotInIsNotReported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k NOT IN (?) ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k NOT IN (?,?) ALLOW FILTERING");
    }

    @Test
    public void testOtherNonExactValueOperatorsAreNotReported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k != ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k < ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k <= ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k > ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k >= ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k BETWEEN ? AND ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k LIKE ? ALLOW FILTERING");
    }

    @Test
    public void testCompositePartitionKeyWithOneNonExactValueColumnIsNotReported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))");
        assertPartitionKeyBindIndexes(new short[]{ 0, 1 }, "SELECT * FROM %s WHERE pk1 = ? AND pk2 = ?");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE pk1 = ? AND pk2 != ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE pk1 = ? AND pk2 NOT IN (?) ALLOW FILTERING");
    }

    @Test
    public void testTwoSlicesOnSamePartitionKeyColumnAreNotReported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k > ? AND k < ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k >= ? AND k <= ? ALLOW FILTERING");
    }

    @Test
    public void testPartitionKeySliceWithClusteringColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        assertPartitionKeyBindIndexes(new short[]{ 0 }, "SELECT * FROM %s WHERE k = ? AND c > ?");
        assertPartitionKeyBindIndexes(new short[]{ 1 }, "SELECT * FROM %s WHERE c > ? AND k = ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k > ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k > ? AND k < ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k > ? AND c = ? ALLOW FILTERING");
    }

    @Test
    public void testCompositePartitionKeyWithSliceIsNotReported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))");
        assertPartitionKeyBindIndexes(new short[]{ 1, 0 }, "SELECT * FROM %s WHERE pk2 = ? AND pk1 = ?");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE pk1 = ? AND pk2 > ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE pk1 > ? AND pk2 = ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE pk1 > ? AND pk2 > ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE pk1 = ? AND pk2 != ? ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE pk1 = ? AND pk2 BETWEEN ? AND ? ALLOW FILTERING");
    }

    @Test
    public void testNamedMarkers() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        assertPartitionKeyBindIndexes(new short[]{ 0 }, "SELECT * FROM %s WHERE k = :a");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k > :a ALLOW FILTERING");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k > :lo AND k < :hi ALLOW FILTERING");
    }

    @Test
    public void testTokenRestrictionIsNotReported() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, v int, PRIMARY KEY ((pk1, pk2)))");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE token(pk1, pk2) > ?");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE token(pk1, pk2) = ?");
    }

    @Test
    public void testModificationStatements() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");
        assertPartitionKeyBindIndexes(new short[]{ 1 }, "UPDATE %s SET v = ? WHERE k = ? AND c = ?");
        assertPartitionKeyBindIndexes(new short[]{ 0 }, "DELETE FROM %s WHERE k = ? AND c > ?");
        assertPartitionKeyBindIndexes(new short[]{ 0 }, "INSERT INTO %s (k, c, v) VALUES (?, ?, ?)");
    }

    @Test
    public void testLikeOnTextPartitionKeyIsNotReported() throws Throwable
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY, v int)");
        assertPartitionKeyBindIndexes(new short[]{ 0 }, "SELECT * FROM %s WHERE k = ?");
        assertPartitionKeyBindIndexes(null, "SELECT * FROM %s WHERE k LIKE ? ALLOW FILTERING");
    }

    private void assertPartitionKeyBindIndexes(short[] expected, String query) throws Throwable
    {
        QueryProcessor.Prepared prepared = QueryProcessor.prepareInternal(formatQuery(query));
        assertArrayEquals(expected, prepared.statement.getPartitionKeyBindVariableIndexes());
    }
}
