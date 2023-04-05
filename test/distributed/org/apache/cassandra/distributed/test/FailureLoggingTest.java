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

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.LogResult;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.reads.range.RangeCommandIterator;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FailureLoggingTest extends TestBaseImpl
{
    private static Cluster cluster;
    
    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CassandraRelevantProperties.FAILURE_LOGGING_INTERVAL_SECONDS.setInt(0);
        cluster = init(Cluster.build(1).withInstanceInitializer(BBRequestFailures::install).start());
        cluster.schemaChange("create table "+KEYSPACE+".tbl (id int primary key, i int)");
    }

    @AfterClass
    public static void tearDownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void resetBootstrappingState()
    {
        cluster.get(1).callOnInstance(() -> BBRequestFailures.bootstrapping = false);
        
    }

    @Test
    public void testRequestBootstrapFail() throws Throwable
    {
        cluster.get(1).callOnInstance(() -> BBRequestFailures.bootstrapping = true);
        long mark = cluster.get(1).logs().mark();

        try
        {
            cluster.coordinator(1).execute("select * from " + KEYSPACE + ".tbl where id = 55", ConsistencyLevel.ALL);
            fail("Query should fail");
        }
        catch (RuntimeException e)
        {
            LogResult<List<String>> result = cluster.get(1).logs().grep(mark, "while executing SELECT");
            assertEquals(1, result.getResult().size());
            assertTrue(result.getResult().get(0).contains("Cannot read from a bootstrapping node"));
        }
    }

    @Test
    public void testRangeRequestFail() throws Throwable
    {
        long mark = cluster.get(1).logs().mark();

        try
        {
            cluster.coordinator(1).execute("select * from " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            fail("Query should fail");
        }
        catch (RuntimeException e)
        {
            LogResult<List<String>> result = cluster.get(1).logs().grep(mark, "while executing SELECT");
            assertEquals(1, result.getResult().size());
            assertTrue(result.getResult().get(0).contains("Cannot achieve consistency level"));
        }
    }

    @Test
    public void testReadRequestFail() throws Throwable
    {
        long mark = cluster.get(1).logs().mark();

        try
        {
            cluster.coordinator(1).execute("select * from " + KEYSPACE + ".tbl where id = 55", ConsistencyLevel.ALL);
            fail("Query should fail");
        }
        catch (RuntimeException e)
        {
            LogResult<List<String>> result = cluster.get(1).logs().grep(mark, "while executing SELECT");
            assertEquals(1, result.getResult().size());
            assertTrue(result.getResult().get(0).contains("Cannot achieve consistency level"));
        }
    }

    public static class BBRequestFailures
    {
        static volatile boolean bootstrapping = false;
        
        static void install(ClassLoader cl, int nodeNumber)
        {
            ByteBuddy bb = new ByteBuddy();
            
            bb.redefine(StorageService.class)
              .method(named("isBootstrapMode"))
              .intercept(MethodDelegation.to(BBRequestFailures.class))
              .make()
              .load(cl, ClassLoadingStrategy.Default.INJECTION);

            bb.redefine(RangeCommandIterator.class)
              .method(named("sendNextRequests"))
              .intercept(MethodDelegation.to(BBRequestFailures.class))
              .make()
              .load(cl, ClassLoadingStrategy.Default.INJECTION);

            bb.redefine(StorageProxy.class)
              .method(named("fetchRows"))
              .intercept(MethodDelegation.to(BBRequestFailures.class))
              .make()
              .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static boolean isBootstrapMode()
        {
            return bootstrapping;
        }

        @SuppressWarnings("unused")
        public static PartitionIterator sendNextRequests()
        {
            throw UnavailableException.create(org.apache.cassandra.db.ConsistencyLevel.ALL, 1, 0);
        }

        @SuppressWarnings("unused")
        public static PartitionIterator fetchRows(List<SinglePartitionReadCommand> commands, 
                                                  org.apache.cassandra.db.ConsistencyLevel consistencyLevel, 
                                                  long queryStartNanoTime)
        {
            throw UnavailableException.create(org.apache.cassandra.db.ConsistencyLevel.ALL, 1, 0);
        }
    }
}