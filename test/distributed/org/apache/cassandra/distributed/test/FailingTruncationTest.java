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

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_BBFAILHELPER_ENABLED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FailingTruncationTest extends TestBaseImpl
{
    @Test
    public void testFailingTruncation() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withInstanceInitializer(BBFailHelper::install)
                                           .start()))
        {
            cluster.setUncaughtExceptionsFilter(t -> "truncateBlocking".equals(t.getMessage()));
            TEST_BBFAILHELPER_ENABLED.setBoolean(true);
            cluster.schemaChange("create table " + KEYSPACE + ".tbl (id int primary key, t int)");
            try
            {
                cluster.coordinator(1).execute("TRUNCATE " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
                fail("Truncate should fail on node 2");
            }
            catch (Exception e)
            {
                assertTrue(e.getMessage().contains("Truncate failed on replica /127.0.0.2"));
            }
        }
    }

    public static class BBFailHelper
    {

        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber == 2)
            {
                new ByteBuddy().redefine(ColumnFamilyStore.class)
                               .method(named("truncateBlocking"))
                               .intercept(MethodDelegation.to(BBFailHelper.class))
                               .make()
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static void truncateBlocking()
        {
            if (TEST_BBFAILHELPER_ENABLED.getBoolean())
                throw new RuntimeException("truncateBlocking");
        }
    }
}
