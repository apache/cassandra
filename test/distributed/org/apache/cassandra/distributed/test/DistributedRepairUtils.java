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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.ResultSet;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.IInvokableInstance;
import org.apache.cassandra.metrics.StorageMetrics;

public final class DistributedRepairUtils
{
    public static final int DEFAULT_COORDINATOR = 1;

    private DistributedRepairUtils()
    {

    }

    public static NodeToolResult repair(AbstractCluster<?> cluster, RepairType repairType, boolean withNotifications, String... args) {
        return repair(cluster, DEFAULT_COORDINATOR, repairType, withNotifications, args);
    }

    public static NodeToolResult repair(AbstractCluster<?> cluster, int node, RepairType repairType, boolean withNotifications, String... args) {
        args = repairType.append(args);
        args = ArrayUtils.addAll(new String[] { "repair" }, args);
        return cluster.get(node).nodetoolResult(withNotifications, args);
    }

    public static <I extends IInvokableInstance, C extends AbstractCluster<I>> long getRepairExceptions(C cluster)
    {
        return getRepairExceptions(cluster, DEFAULT_COORDINATOR);
    }

    public static <I extends IInvokableInstance, C extends AbstractCluster<I>> long getRepairExceptions(C cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> StorageMetrics.repairExceptions.getCount());
    }

    public static ResultSet queryParentRepairHistory(AbstractCluster<?> cluster, String ks, String table)
    {
        return queryParentRepairHistory(cluster, DEFAULT_COORDINATOR, ks, table);
    }

    public static ResultSet queryParentRepairHistory(AbstractCluster<?> cluster, int coordinator, String ks, String table)
    {
        // This is kinda brittle since the caller never gets the ID and can't ask for the ID; it needs to infer the id
        // this logic makes the assumption the ks/table pairs are unique (should be or else create should fail) so any
        // repair for that pair will be the repair id
        Set<String> tableNames = table == null? Collections.emptySet() : ImmutableSet.of(table);
        ResultSet rs = null;
        Exception latestException = null;
        for (int i = 0; i < 10; i++)
        {
            try
            {
                rs = cluster.coordinator(coordinator)
                            .executeWithResult("SELECT * FROM system_distributed.parent_repair_history", ConsistencyLevel.QUORUM)
                            .filter(row -> ks.equals(row.getString("keyspace_name")))
                            .filter(row -> tableNames.equals(row.getSet("columnfamily_names")));
                break;
            }
            catch (Exception e)
            {
                latestException = e;
                rs = null;
                //TODO do we have a backoff stategy I can leverage?  I would prefer expotential but don't want to add for somethinr minor
                Uninterruptibles.sleepUninterruptibly( (i + 1) * 300, TimeUnit.MILLISECONDS);
            }
        }
        if (rs == null)
        {
            // exception should exist
            if (latestException == null)
            {
                Assert.fail("Unable to query system_distributed.parent_repair_history, got back neither result set or exception ");
            }
            if (latestException instanceof RuntimeException)
                throw (RuntimeException) latestException;
            throw new RuntimeException(latestException);
        }
        return rs;
    }

    public static void assertParentRepairNotExist(AbstractCluster<?> cluster, String ks, String table)
    {
        assertParentRepairNotExist(cluster, DEFAULT_COORDINATOR, ks, table);
    }

    public static void assertParentRepairNotExist(AbstractCluster<?> cluster, int coordinator, String ks, String table)
    {
        ResultSet rs = queryParentRepairHistory(cluster, coordinator, ks, table);
        Assert.assertFalse("No repairs should be found but at least one found", rs.hasNext());
    }

    public static void assertParentRepairNotExist(AbstractCluster<?> cluster, String ks)
    {
        assertParentRepairNotExist(cluster, DEFAULT_COORDINATOR, ks);
    }

    public static void assertParentRepairNotExist(AbstractCluster<?> cluster, int coordinator, String ks)
    {
        ResultSet rs = queryParentRepairHistory(cluster, coordinator, ks, null);
        Assert.assertFalse("No repairs should be found but at least one found", rs.hasNext());
    }

    public static void assertParentRepairSuccess(AbstractCluster<?> cluster, String ks, String table)
    {
        assertParentRepairSuccess(cluster, DEFAULT_COORDINATOR, ks, table);
    }

    public static void assertParentRepairSuccess(AbstractCluster<?> cluster, int coordinator, String ks, String table)
    {
        ResultSet rs = queryParentRepairHistory(cluster, coordinator, ks, table);
        validateExistingParentRepair(rs, row -> {
            // check completed
            Assert.assertNotNull("finished_at not found, the repair is not complete?", rs.getTimestamp("finished_at"));

            // check not failed (aka success)
            Assert.assertNull("Exception found", rs.getString("exception_stacktrace"));
            Assert.assertNull("Exception found", rs.getString("exception_message"));
        });
    }

    public static void assertParentRepairFailedWithMessageContains(AbstractCluster<?> cluster, String ks, String table, String message)
    {
        assertParentRepairFailedWithMessageContains(cluster, DEFAULT_COORDINATOR, ks, table, message);
    }

    public static void assertParentRepairFailedWithMessageContains(AbstractCluster<?> cluster, int coordinator, String ks, String table, String message)
    {
        ResultSet rs = queryParentRepairHistory(cluster, coordinator, ks, table);
        validateExistingParentRepair(rs, row -> {
            // check completed
            Assert.assertNotNull("finished_at not found, the repair is not complete?", rs.getTimestamp("finished_at"));

            // check failed
            Assert.assertNotNull("Exception not found", rs.getString("exception_stacktrace"));
            String exceptionMessage = rs.getString("exception_message");
            Assert.assertNotNull("Exception not found", exceptionMessage);

            Assert.assertTrue("Unable to locate message '" + message + "' in repair error message: " + exceptionMessage, exceptionMessage.contains(message));
        });
    }

    private static void validateExistingParentRepair(ResultSet rs, Consumer<Row> fn)
    {
        Assert.assertTrue("No rows found", rs.hasNext());
        Row row = rs.next();

        Assert.assertNotNull("parent_id (which is the primary key) was null", row.getUUID("parent_id"));

        fn.accept(row);

        // make sure no other records found
        Assert.assertFalse("Only one repair expected, but found more than one", rs.hasNext());
    }

    public enum RepairType {
        FULL {
            public String[] append(String... args)
            {
                return ArrayUtils.add(args, "--full");
            }
        },
        INCREMENTAL {
            public String[] append(String... args)
            {
                // incremental is the default
                return args;
            }
        },
        PREVIEW {
            public String[] append(String... args)
            {
                return ArrayUtils.addAll(args, "--preview");
            }
        };

        public abstract String[] append(String... args);
    }

    public enum RepairParallelism {
        SEQUENTIAL {
            public String[] append(String... args)
            {
                return ArrayUtils.add(args, "--sequential");
            }
        },
        PARALLEL {
            public String[] append(String... args)
            {
                // default is to be parallel
                return args;
            }
        },
        DATACENTER_AWARE {
            public String[] append(String... args)
            {
                return ArrayUtils.add(args, "--dc-parallel");
            }
        };

        public abstract String[] append(String... args);
    }
}
