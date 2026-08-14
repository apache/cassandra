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

package org.apache.cassandra.index.sai.disk;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v2.V2RowAwarePrimaryKeyFactory;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class RowAwarePrimaryKeyTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "version={0}")
    public static List<Object[]> data()
    {
        return Version.ALL.stream()
                          .filter(v -> v.onDiskFormat().indexFeatureSet().isRowAware())
                          .map(v -> new Object[]{ v })
                          .collect(Collectors.toList());
    }

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @Test
    public void testHashCodeForDeferredPrimaryKey()
    {
        PrimaryKey.Factory factory = version.onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

        // Test relies on this implementation detail
        assertTrue(factory instanceof V2RowAwarePrimaryKeyFactory);

        // Set up the primary key
        Token token = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key = new BufferDecoratedKey(token, ByteBuffer.allocate(1));
        Supplier<PrimaryKey> supplier = () -> factory.create(key, Clustering.EMPTY);
        PrimaryKey primaryKey1 = factory.createDeferred(token, supplier);

        // Verify the results
        int hash1 = primaryKey1.hashCode();
        // Equals triggers loading the primary key
        assertEquals(primaryKey1, primaryKey1);
        assertEquals(hash1, primaryKey1.hashCode());

        // Do again with explicit loading
        PrimaryKey primaryKey2 = factory.createDeferred(token, supplier);
        int hash2 = primaryKey2.hashCode();
        primaryKey2.loadDeferred();
        assertEquals(hash2, primaryKey2.hashCode());
    }

    @Test
    public void testHashCodeForLoadedPrimaryKey()
    {
        PrimaryKey.Factory factory = version.onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

        // Test relies on this implementation detail
        assertTrue(factory instanceof V2RowAwarePrimaryKeyFactory);

        // Set up the primary key
        Token token1 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key1 = new BufferDecoratedKey(token1, ByteBuffer.allocate(1));
        PrimaryKey primaryKey1 = factory.create(key1, Clustering.EMPTY);

        // Create equivalent PK
        Token token2 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key2 = new BufferDecoratedKey(token2, ByteBuffer.allocate(1));
        PrimaryKey primaryKey2 = factory.create(key2, Clustering.EMPTY);

        assertEquals(primaryKey1.hashCode(), primaryKey2.hashCode());
    }

    @Test
    public void testHashCodeForDeferedPrimaryKeyWithClusteringColumns()
    {
        ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);
        PrimaryKey.Factory factory = version.onDiskFormat().newPrimaryKeyFactory(comparator);

        // Test relies on this implementation detail
        assertTrue(factory instanceof V2RowAwarePrimaryKeyFactory);

        // Set up the primary key
        Token token1 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key1 = new BufferDecoratedKey(token1, ByteBuffer.allocate(1));
        PrimaryKey primaryKey1 = factory.create(key1, Clustering.make(ByteBuffer.allocate(1)));

        // Create equivalent PK
        Token token2 = new Murmur3Partitioner.LongToken(1);
        DecoratedKey key2 = new BufferDecoratedKey(token2, ByteBuffer.allocate(1));
        PrimaryKey primaryKey2 = factory.create(key2, Clustering.make(ByteBuffer.allocate(1)));

        assertEquals(primaryKey1.hashCode(), primaryKey2.hashCode());
    }

    @Test
    public void testComparisonBetweenTokenOnlyAndFullKey()
    {
        PrimaryKey.Factory factory = version.onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

        // Test relies on this implementation detail
        assertTrue(factory instanceof V2RowAwarePrimaryKeyFactory);

        // Create keys with the same token
        Token token = new Murmur3Partitioner.LongToken(100);
        DecoratedKey decoratedKey = new BufferDecoratedKey(token, ByteBuffer.allocate(1));

        PrimaryKey tokenOnlyKey = factory.createTokenOnly(token);
        PrimaryKey fullKey = factory.create(decoratedKey, Clustering.EMPTY);

        // When tokens are equal, token-only key should compare equal to full key.
        // This is the critical behavior tested in V2RowAwarePrimaryKeyFactory.compareTo.
        assertEquals(0, tokenOnlyKey.compareTo(fullKey));
        assertEquals(0, fullKey.compareTo(tokenOnlyKey));

        // They should be considered equal
        assertEquals(tokenOnlyKey, fullKey);
        assertEquals(fullKey, tokenOnlyKey);
    }

    @Test
    public void testComparisonBetweenTokenOnlyKeysWithDifferentTokens()
    {
        PrimaryKey.Factory factory = version.onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

        // Test relies on this implementation detail
        assertTrue(factory instanceof V2RowAwarePrimaryKeyFactory);

        // Create token-only keys with different tokens
        Token token1 = new Murmur3Partitioner.LongToken(50);
        Token token2 = new Murmur3Partitioner.LongToken(100);

        PrimaryKey tokenOnlyKey1 = factory.createTokenOnly(token1);
        PrimaryKey tokenOnlyKey2 = factory.createTokenOnly(token2);

        // Comparison should be based solely on tokens
        assertThat(tokenOnlyKey1.compareTo(tokenOnlyKey2)).isLessThan(0);
        assertThat(tokenOnlyKey2.compareTo(tokenOnlyKey1)).isGreaterThan(0);

        // They should not be equal
        assertNotEquals(tokenOnlyKey1, tokenOnlyKey2);
    }

    @Test
    public void testComparisonWithClusteringColumns()
    {
        ClusteringComparator comparator = new ClusteringComparator(Int32Type.instance);
        PrimaryKey.Factory factory = version.onDiskFormat().newPrimaryKeyFactory(comparator);

        // Test relies on this implementation detail
        assertTrue(factory instanceof V2RowAwarePrimaryKeyFactory);

        // Create keys with the same token but different clustering
        Token token = new Murmur3Partitioner.LongToken(100);
        DecoratedKey decoratedKey = new BufferDecoratedKey(token, ByteBuffer.allocate(1));

        PrimaryKey tokenOnlyKey = factory.createTokenOnly(token);
        PrimaryKey fullKeyWithClustering = factory.create(decoratedKey, Clustering.make(Int32Type.instance.decompose(1)));

        // When one key is token-only, comparison should be based on tokens only
        // even if the other key has clustering columns
        assertEquals(0, tokenOnlyKey.compareTo(fullKeyWithClustering));
        assertEquals(0, fullKeyWithClustering.compareTo(tokenOnlyKey));
    }

    @Test
    public void testTokenOnlyKeyHashCode()
    {
        PrimaryKey.Factory factory = version.onDiskFormat().newPrimaryKeyFactory(EMPTY_COMPARATOR);

        // Test relies on this implementation detail
        assertTrue(factory instanceof V2RowAwarePrimaryKeyFactory);

        // Create token-only keys with the same token
        Token token = new Murmur3Partitioner.LongToken(42);
        PrimaryKey tokenOnlyKey1 = factory.createTokenOnly(token);
        PrimaryKey tokenOnlyKey2 = factory.createTokenOnly(token);

        // Hash codes should be equal for equal token-only keys
        assertEquals(tokenOnlyKey1.hashCode(), tokenOnlyKey2.hashCode());

        // Hash code should be based on the token
        assertEquals(token.hashCode(), tokenOnlyKey1.hashCode());
    }
}
