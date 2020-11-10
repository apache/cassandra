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
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.config.Config.DiskFailurePolicy;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.io.sstable.format.big.BigTableRowIndexEntry;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableCallable;
import org.apache.cassandra.distributed.shared.AbstractBuilder;
import org.apache.cassandra.distributed.shared.NetworkTopology;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.ForwardingSSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class JVMStabilityInspectorCorruptSSTableExceptionTest extends TestBaseImpl
{
    @Test
    public void testAbstractLocalAwareExecutorServiceOnIgnoredDiskFailurePolicy() throws Exception
    {
        test(DiskFailurePolicy.ignore, true, true);
    }

    @Test
    public void testAbstractLocalAwareExecutorServiceOnStopParanoidDiskFailurePolicy() throws Exception
    {
        test(DiskFailurePolicy.stop_paranoid, false, false);
    }

    private static void test(DiskFailurePolicy policy, boolean expectNativeTransportRunning, boolean expectGossiperEnabled) throws Exception
    {
        String table = policy.name();
        try (final Cluster cluster = init(getCluster(policy).start()))
        {
            IInvokableInstance node = cluster.get(1);
            boolean[] setup = node.callOnInstance(() -> {
                CassandraDaemon instanceForTesting = CassandraDaemon.getInstanceForTesting();
                instanceForTesting.completeSetup();
                StorageService.instance.registerDaemon(instanceForTesting);
                return new boolean[]{ StorageService.instance.isNativeTransportRunning(), Gossiper.instance.isEnabled() };
            });

            // make sure environment is setup propertly
            Assert.assertTrue("Native support is not running, test is not ready!", setup[0]);
            Assert.assertTrue("Gossiper is not running, test is not ready!", setup[1]);

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + "." + table + " (id bigint PRIMARY KEY)");
            node.executeInternal("INSERT INTO " + KEYSPACE + "." + table + " (id) VALUES (?)", 0L);
            corruptTable(node, KEYSPACE, table);

            try
            {
                cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + '.' + table + " WHERE id=?", ConsistencyLevel.ONE, 0L);
                Assert.fail("Select should fail as we corrupted SSTable on purpose.");
            }
            catch (final Exception ex)
            {
                // we expect that above query fails as we corrupted an sstable
            }

            waitForStop(!expectGossiperEnabled, node, new SerializableCallable<Boolean>()
            {
                public Boolean call()
                {
                    return Gossiper.instance.isEnabled();
                }
            });

            waitForStop(!expectNativeTransportRunning, node, new SerializableCallable<Boolean>()
            {
                public Boolean call()
                {
                    return StorageService.instance.isNativeTransportRunning();
                }
            });
        }
    }

    private static void waitForStop(boolean shouldWaitForStop,
                                    IInvokableInstance node,
                                    SerializableCallable<Boolean> serializableCallable) throws Exception
    {
        int attempts = 3;
        boolean running = true;

        while (attempts > 0 && running)
        {
            try
            {
                running = node.callOnInstance(serializableCallable);
                attempts--;
            }
            catch (final NoClassDefFoundError ex)
            {
                // gossiper throws this
                Assert.assertEquals("Could not initialize class org.apache.cassandra.service.StorageService", ex.getMessage());
                running = false;
            }
            catch (final ExceptionInInitializerError ex)
            {
                // native thows this, ignore on purpose, this means that native transport is closed.
                running = false;
            }

            Thread.sleep(5000);
        }

        if (shouldWaitForStop && running)
        {
            Assert.fail("we did want a service to stop, but it did not.");
        }

        if (!shouldWaitForStop && !running)
        {
            Assert.fail("we did not want a service to stop, but it did.");
        }
    }

    private static void corruptTable(IInvokableInstance node, String keyspace, String table)
    {
        node.runOnInstance(() -> {
            ColumnFamilyStore cf = Keyspace.open(keyspace).getColumnFamilyStore(table);
            cf.forceBlockingFlush();

            Set<SSTableReader> remove = cf.getLiveSSTables();
            Set<SSTableReader> replace = new HashSet<>();
            for (SSTableReader r : remove)
                replace.add(new CorruptedSSTableReader(r));

            cf.getTracker().removeUnsafe(remove);
            cf.addSSTables(replace);
        });
    }

    private static AbstractBuilder<IInvokableInstance, Cluster, Cluster.Builder> getCluster(DiskFailurePolicy diskFailurePolicy)
    {
        return Cluster.build()
                      .withNodeIdTopology(NetworkTopology.singleDcNetworkTopology(1, "dc0", "rack0"))
                      .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                  .set("disk_failure_policy", diskFailurePolicy.name()));
    }

    private static final class CorruptedSSTableReader extends ForwardingSSTableReader
    {
        public CorruptedSSTableReader(SSTableReader delegate)
        {
            super(delegate);
        }

        @Override
        public UnfilteredRowIterator iterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
        {
            throw throwCorrupted();
        }

        private CorruptSSTableException throwCorrupted()
        {
            throw new CorruptSSTableException(new IOException("failed to get position"), descriptor.baseFilename());
        }
    }
}
