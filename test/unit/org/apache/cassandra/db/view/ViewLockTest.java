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

package org.apache.cassandra.db.view;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ViewLockTest extends CQLTester
{
    @Test
    public void testInterruptedApplyReleasesAcquiredLocks() throws Throwable
    {
        List<TableMetadata> tables = new ArrayList<>();
        for (int i = 0; i < 3; i++)
        {
            createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
            createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
            tables.add(getCurrentColumnFamilyStore().metadata());
        }

        for (boolean droppable : new boolean[]{ false, true })
        {
            // the locks are weakly referenced, so hold on to the ones apply will use
            List<Lock> locks = null;
            Mutation mutation = null;
            for (int key = 0; locks == null; key++)
            {
                mutation = mutation(tables, key);
                locks = lockAll(mutation);
            }

            // keep the lock of the last table, so apply acquires the others and then has to retry
            List<Lock> acquired = locks.subList(0, locks.size() - 1);
            acquired.forEach(Lock::unlock);
            Mutation write = mutation;
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread writer = new Thread(() -> {
                Thread.currentThread().interrupt();
                try
                {
                    Keyspace.open(KEYSPACE).apply(write, false, true, droppable);
                }
                catch (Throwable t)
                {
                    failure.set(t);
                }
            });
            writer.start();
            try
            {
                writer.join(10000);
                assertFalse("Writer did not return", writer.isAlive());
            }
            finally
            {
                locks.get(locks.size() - 1).unlock();
            }

            assertThat(failure.get()).isInstanceOf(UncheckedInterruptedException.class)
                                     .hasCauseInstanceOf(InterruptedException.class);
            for (Lock lock : acquired)
            {
                assertTrue("Acquired view lock must be released when apply is interrupted", lock.tryLock());
                lock.unlock();
            }
        }
    }

    @Test
    public void testApplyWithDroppedTableReleasesLocks() throws Throwable
    {
        List<TableMetadata> tables = new ArrayList<>();
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        createViewAsync("CREATE MATERIALIZED VIEW %s AS SELECT * FROM %s WHERE k IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, k)");
        tables.add(getCurrentColumnFamilyStore().metadata());
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        tables.add(getCurrentColumnFamilyStore().metadata());

        List<Lock> locks = null;
        Mutation mutation = null;
        for (int key = 0; locks == null; key++)
        {
            mutation = mutation(tables, key);
            locks = lockAll(mutation);
        }
        locks.forEach(Lock::unlock);
        dropTable("DROP TABLE %s");

        for (boolean async : new boolean[]{ false, true })
        {
            for (boolean allowConflicts : new boolean[]{ false, true })
            {
                Mutation write = mutation(tables, mutation.key().getKey().getInt(0));
                if (allowConflicts)
                    write.allowPotentialTransactionConflicts();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread writer = new Thread(() -> {
                    try
                    {
                        if (async)
                            Keyspace.open(KEYSPACE).applyFuture(write, false, true).get();
                        else
                            Keyspace.open(KEYSPACE).apply(write, false, true, true);
                    }
                    catch (Throwable t)
                    {
                        failure.set(t);
                    }
                });
                writer.start();
                writer.join(10000);
                assertFalse("Writer did not return", writer.isAlive());

                for (Lock lock : locks)
                {
                    assertTrue("Acquired view lock must be released when a table has been dropped", lock.tryLock());
                    lock.unlock();
                }
                if (allowConflicts)
                {
                    if (failure.get() != null)
                        throw failure.get();
                    assertRows(execute("SELECT k, v FROM " + KEYSPACE + '.' + tables.get(0).name), row(write.key().getKey().getInt(0), 1));
                }
                else
                    assertThat(failure.get()).isInstanceOf(InvalidRequestException.class);
            }
        }
    }

    private static Mutation mutation(List<TableMetadata> tables, int key)
    {
        DecoratedKey partitionKey = tables.get(0).partitioner.decorateKey(ByteBufferUtil.bytes(key));
        Mutation.SimpleBuilder builder = Mutation.simpleBuilder(KEYSPACE, partitionKey);
        for (TableMetadata table : tables)
            builder.update(table).row().add("v", 1);
        return builder.build();
    }

    /**
     * Acquires the view lock of every table in the mutation, in apply's order, or returns null if two tables share a lock.
     */
    private static List<Lock> lockAll(Mutation mutation)
    {
        List<Lock> locks = new ArrayList<>();
        for (TableId tableId : mutation.getTableIds())
        {
            Lock lock = ViewManager.acquireLockFor(Objects.hash(mutation.key().getKey(), tableId));
            assertNotNull(lock);
            locks.add(lock);
        }
        if (new HashSet<>(locks).size() == locks.size())
            return locks;
        locks.forEach(Lock::unlock);
        return null;
    }
}
