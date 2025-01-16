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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExpandCQLTest extends TestBaseImpl
{
    @Test
    public void testCreateTable() throws IOException
    {
        createHelper(TableMetadata.class, withKeyspace("create table %s.x (id int primary key)"));
    }

    @Test
    public void testCreateKeyspace() throws IOException
    {
        createHelper(KeyspaceMetadata.class, "create keyspace abc with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    @Test
    public void testCreateIndex() throws IOException
    {
        createHelper(IndexMetadata.class, withKeyspace("create index abc on %s.t (x)"));
    }

    private void createHelper(Class<?> clazz, String query) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withInstanceInitializer((cl, i) -> BBTable.install(cl, clazz))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.t (id int primary key, x int)"));
            try
            {
                cluster.get(1).runOnInstance(() -> BBTable.enabled.set(true));
                cluster.schemaChange(withKeyspace(query));
                fail("expected exception");
            }
            catch (Exception e)
            {
                assertEquals("SyntaxException", e.getClass().getSimpleName());
            }
            cluster.get(1).runOnInstance(() -> BBTable.enabled.set(false));
            cluster.schemaChange(withKeyspace(query));
        }
    }

    public static class BBTable
    {
        static AtomicBoolean enabled = new AtomicBoolean();
        static void install(ClassLoader cl, Class<?> c)
        {
            new ByteBuddy().rebase(c)
                           .method(named("toCqlString"))
                           .intercept(MethodDelegation.to(BBTable.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static String toCqlString(@SuperCall Callable<String> zuper) throws Exception
        {
            if (!enabled.get())
                return zuper.call();
            return zuper.call().toLowerCase().replace("c", "x");
        }
    }

}
