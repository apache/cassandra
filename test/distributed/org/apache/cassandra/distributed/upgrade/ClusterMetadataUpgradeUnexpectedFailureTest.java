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

package org.apache.cassandra.distributed.upgrade;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Constants;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.migration.CMSInitializationException;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.schema.SchemaConstants.METADATA_KEYSPACE_NAME;
import static org.apache.cassandra.schema.SchemaConstants.SCHEMA_KEYSPACE_NAME;
import static org.apache.cassandra.schema.SchemaKeyspaceTables.COLUMNS;
import static org.apache.cassandra.schema.SchemaKeyspaceTables.KEYSPACES;
import static org.apache.cassandra.schema.SchemaKeyspaceTables.TABLES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataUpgradeUnexpectedFailureTest extends UpgradeTestBase
{
    @Test
    public void upgradeFailsUnexpectedlyBeforePreInitialize() throws Throwable
    {
        testCMSInitializationError(true, false, "Something unexpected went wrong");
    }

    @Test
    public void upgradeFailsUnexpectedlyBeforeInitialize() throws Throwable
    {
        testCMSInitializationError(false, true, "CMS initialization failed, see logs for details");
    }

    private void testCMSInitializationError(boolean failBeforePreInit,
                                            boolean failAfterPreInit,
                                            String expectedError) throws Throwable
    {
        Consumer<UpgradeableCluster.Builder > builderUpdater = builder -> builder.withInstanceInitializer(BBInstaller::installUpgradeVersionBB);
        new TestCase()
            .nodes(3)
            .nodesToUpgrade(1, 2, 3)
            .withConfig((cfg) -> cfg.with(Feature.NETWORK, Feature.GOSSIP)
                                    .set(Constants.KEY_DTEST_FULL_STARTUP, true))
            .upgradesToCurrentFrom(v41)
            .withBuilder(builderUpdater)
            .setup((cluster) -> {
                BBState.failBeforePreInitialize.set(failBeforePreInit);
                BBState.failAfterPreInitialize.set(failAfterPreInit);
                cluster.schemaChange(withKeyspace("ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor':2}"));
                cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
                for (int i = 0; i < 10; i++)
                {
                    cluster.get(1).coordinator().execute("INSERT into " + KEYSPACE + ".tbl (pk, ck, v) VALUES (?, ?, ?)",
                                                         ConsistencyLevel.ALL,
                                                         i, i, i);
                }
            })
            .runAfterClusterUpgrade((cluster) -> {
                String oldHostId = getHostId(cluster.get(1));
                // we injected a BB helper to trigger an unexpected failure on the first attempt at initialization.
                // i.e. an exception that must be caught as opposed to a mismatch in metadata or down peer.
                cluster.get(1).nodetoolResult("cms", "initialize").asserts().failure().errorContains(expectedError);
                assertEquals(oldHostId, getHostId(cluster.get(1)));

                // handling the failure should have included cleaning up any state so that another attempt can be
                // made, which this time should succeed.
                for(IUpgradeableInstance inst : cluster)
                    inst.nodetoolResult("cms").asserts().success().stdoutContains("Service State: GOSSIP");
                // Basic smoke test
                for (int i = 0; i < 10; i++)
                {
                    Object [][] rows = cluster.get(1)
                                              .coordinator()
                                              .execute("SELECT v from " + KEYSPACE + ".tbl WHERE pk = ?",
                                                       ConsistencyLevel.ALL, i);
                    assertEquals(1, rows.length);
                    assertEquals(i, rows[0][0]);
                }

                // Make sure that no trace of the metadata keyspace is present after the CMS initialization failure
                assertSchemaTablesContent(cluster, true);

                Object[][] rows = cluster.get(1).coordinator().execute("DESCRIBE FULL SCHEMA", ConsistencyLevel.NODE_LOCAL);
                for (Object[] row : rows)
                    assertFalse(row[0].toString().equalsIgnoreCase(METADATA_KEYSPACE_NAME));

                assertEquals(oldHostId, getHostId(cluster.get(1)));
                // A subsequent initialization should succeed
                cluster.get(1).nodetoolResult("cms", "initialize").asserts().success();
                assertNotEquals(oldHostId, getHostId(cluster.get(1)));
                assertSchemaTablesContent(cluster, false);
            }).run();
    }

    private static void assertSchemaTablesContent(UpgradeableCluster cluster, boolean expectEmpty)
    {
        for (String schemaTable : new String[] { KEYSPACES, TABLES, COLUMNS })
        {
            Object[][] rows = cluster.get(1)
                                     .coordinator()
                                     .execute("SELECT * FROM " + SCHEMA_KEYSPACE_NAME + "." + schemaTable + " WHERE keyspace_name = ?" ,
                                              ConsistencyLevel.ALL, METADATA_KEYSPACE_NAME);
            if (expectEmpty)
                assertEquals(0, rows.length);
            else
                assertTrue(rows.length >= 1);
        }
    }

    private static String getHostId(IUpgradeableInstance i)
    {
        return ((IInvokableInstance)i).callOnInstance(() -> SystemKeyspace.getLocalHostId().toString());
    }

    public static class BBInstaller
    {
        public static void installUpgradeVersionBB(ClassLoader classLoader, Integer num)
        {
            try
            {
                // Fail before the LocalLog is initialized with the PRE_INITIALIZE_CMS
                new ByteBuddy().rebase(LocalLog.class)
                               .method(named("bootstrap"))
                               .intercept(MethodDelegation.to(BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
                // Fail after the PRE_INITIALIZE_CMS has been enacted, as the INITIALIZE_CMS is being committed
                new ByteBuddy().rebase(ClusterMetadataService.class)
                               .method(named("commit"))
                               .intercept(MethodDelegation.to(BBInterceptor.class))
                               .make()
                               .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            }
            catch (NoClassDefFoundError noClassDefFoundError)
            {
                throw noClassDefFoundError;
            }
            catch (Throwable tr)
            {
                throw tr;
            }
        }
    }

    @Shared
    public static class BBState
    {
        public static AtomicBoolean failBeforePreInitialize = new AtomicBoolean(false);
        public static AtomicBoolean failAfterPreInitialize = new AtomicBoolean(false);
    }

    public static class BBInterceptor
    {

        @SuppressWarnings("unused")
        public static void bootstrap(InetAddressAndPort addr, String datacenterSet, @SuperCall Callable<Void> zuper)
        {
            if (BBState.failBeforePreInitialize.getAndSet(false))
                throw new IllegalStateException("Something unexpected went wrong");

            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public static ClusterMetadata commit(@SuperCall Callable<ClusterMetadata> zuper)
        {
            if (BBState.failAfterPreInitialize.getAndSet(false))
                throw new CMSInitializationException();
            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
