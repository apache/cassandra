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

package org.apache.cassandra.service;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.throttler.dynamic.CassandraResourceUtilization;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class RateLimiterTest extends CQLTester {
    private static final String KEYSPACE = "ks_for_rate_limiter";
    private static final String TABLE = "table";

    private static TableMetadata metadata;
    private static DecoratedKey key;
    private static CassandraResourceUtilization originalCassandraRescourceUtilization = CassandraResourceUtilization.instance;

    private CassandraResourceUtilization mockCassResrcUtil;

    public RateLimiterTest()
    {
        requireNetwork();
    }

    @BeforeClass
    public static void defineSchema()
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        metadata =
                TableMetadata.builder(KEYSPACE, TABLE)
                        .addPartitionKeyColumn("pk", UTF8Type.instance)
                        .addClusteringColumn("ck", UTF8Type.instance)
                        .addRegularColumn("rc", UTF8Type.instance)
                        .build();
        key = Murmur3Partitioner.instance.decorateKey(bytes("key"));

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), metadata);
    }

    @Before
    public void mockUserThrottle()
    {
        mockCassResrcUtil = spy(originalCassandraRescourceUtilization);
        CassandraResourceUtilization.instance = mockCassResrcUtil;
        when(mockCassResrcUtil.throttleUserTraffic(eq(KEYSPACE), anyBoolean())).thenReturn(true);
    }

    @After
    public void resetUserThrottle()
    {
        verify(mockCassResrcUtil).throttleUserTraffic(eq(KEYSPACE), anyBoolean());
        CassandraResourceUtilization.instance = originalCassandraRescourceUtilization;
    }

    @Test(expected = OverloadedException.class)
    public void testRateLimiterOverloadThrowInCAS()
    {
        try
        {
            StorageProxy.cas(KEYSPACE, TABLE, null, null, ConsistencyLevel.SERIAL,
                    ConsistencyLevel.ALL, null, 1, 0);
        }
        catch (OverloadedException e)
        {
            Assert.assertTrue(e.getMessage().contains(CassandraResourceUtilization.THROW_MESSAGE));
            throw e;
        }
    }

    @Test(expected = OverloadedException.class)
    public void testRateLimiterOverloadThrowInMutate()
    {
        try
        {
            StorageProxy.mutate(Collections.singletonList(createMutation()), ConsistencyLevel.ALL, 0);
        }
        catch (OverloadedException e)
        {
            Assert.assertTrue(e.getMessage().contains(CassandraResourceUtilization.THROW_MESSAGE));
            throw e;
        }
    }

    @Test(expected = OverloadedException.class)
    public void testRateLimiterOverloadThrowInMutateAtomically()
    {
        try
        {
            StorageProxy.mutateAtomically(Collections.singletonList(createMutation()), ConsistencyLevel.ALL, false, 0);
        }
        catch (OverloadedException e)
        {
            Assert.assertTrue(e.getMessage().contains(CassandraResourceUtilization.THROW_MESSAGE));
            throw e;
        }
    }

    @Test(expected = OverloadedException.class)
    public void testRateLimiterOverloadThrowInReadWithPaxos()
    {
        try
        {
            StorageProxy.readWithPaxos(createReadQuery(), ConsistencyLevel.ALL, ClientState.forInternalCalls(), 0);
        }
        catch (OverloadedException e)
        {
            Assert.assertTrue(e.getMessage().contains(CassandraResourceUtilization.THROW_MESSAGE));
            throw e;
        }
    }

    @Test(expected = OverloadedException.class)
    public void testRateLimiterOverloadThrowInReadRegular()
    {
        try
        {
            StorageProxy.readRegular(createReadQuery(), ConsistencyLevel.ALL, 0);
        }
        catch (OverloadedException e)
        {
            Assert.assertTrue(e.getMessage().contains(CassandraResourceUtilization.THROW_MESSAGE));
            throw e;
        }
    }

    private Mutation createMutation()
    {
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(KEYSPACE, key);
        builder.update(metadata)
                .timestamp(0)
                .row("ck_1")
                .add("rc", "value0");

        return builder.build();
    }

    private SinglePartitionReadCommand.Group createReadQuery()
    {
        int nowInSeconds = FBUtilities.nowInSeconds();
        ColumnFilter columnFilter = ColumnFilter.allRegularColumnsBuilder(metadata, false).build();
        RowFilter rowFilter = RowFilter.create();
        Slice slice = Slice.make(BufferClusteringBound.BOTTOM, BufferClusteringBound.TOP);
        ClusteringIndexSliceFilter sliceFilter = new ClusteringIndexSliceFilter(Slices.with(metadata.comparator, slice), false);
        return SinglePartitionReadCommand.Group.one(SinglePartitionReadCommand.create(metadata, nowInSeconds, columnFilter, rowFilter, DataLimits.NONE, key, sliceFilter));
    }
}
