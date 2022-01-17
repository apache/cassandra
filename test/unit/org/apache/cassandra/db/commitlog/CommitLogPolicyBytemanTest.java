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

package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.KillerForTests;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.awaitility.Awaitility.await;

@RunWith(BMUnitRunner.class)
public class CommitLogPolicyBytemanTest
{
    protected static final String KEYSPACE = "CommitLogBytemanTest";
    protected static final String STANDARD = "Standard";
    private static final String CUSTOM = "Custom";

    private static JVMStabilityInspector.Killer oldKiller;
    private static KillerForTests testKiller;
    private static Config.CommitFailurePolicy oldPolicy;

    public static volatile boolean failSync = false;

    @BeforeClass
    public static void beforeClass() throws ConfigurationException
    {
        // Disable durable writes for system keyspaces to prevent system mutations, e.g. sstable_activity,
        // to end up in CL segments and cause unexpected results in this test wrt counting CL segments,
        // see CASSANDRA-12854
        KeyspaceParams.DEFAULT_LOCAL_DURABLE_WRITES = false;

        SchemaLoader.prepareServer();
        StorageService.instance.getTokenMetadata().updateHostId(UUID.randomUUID(), FBUtilities.getBroadcastAddressAndPort());

        MemtableParams skipListMemtable = MemtableParams.fromMap(ImmutableMap.of("class", "SkipListMemtable"));

        TableMetadata.Builder custom =
                TableMetadata.builder(KEYSPACE, CUSTOM)
                        .addPartitionKeyColumn("k", IntegerType.instance)
                        .addClusteringColumn("c1", MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false))
                        .addClusteringColumn("c2", SetType.getInstance(UTF8Type.instance, false))
                        .addStaticColumn("s", IntegerType.instance)
                        .memtable(skipListMemtable);

        SchemaLoader.createKeyspace(KEYSPACE,
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD(KEYSPACE, STANDARD, 0, AsciiType.instance, BytesType.instance).memtable(skipListMemtable),
                custom);
        CompactionManager.instance.disableAutoCompaction();

        testKiller = new KillerForTests();

        // While we don't want the JVM to be nuked from under us on a test failure, we DO want some indication of
        // an error. If we hit a "Kill the JVM" condition while working with the CL when we don't expect it, an aggressive
        // KillerForTests will assertion out on us.
        oldKiller = JVMStabilityInspector.replaceKiller(testKiller);

        oldPolicy = DatabaseDescriptor.getCommitFailurePolicy();
    }

    @AfterClass
    public static void afterClass()
    {
        JVMStabilityInspector.replaceKiller(oldKiller);
    }

    @Before
    public void beforeTest() throws IOException
    {
        CommitLog.instance.resetUnsafe(true);
    }

    @After
    public void afterTest()
    {
        DatabaseDescriptor.setCommitFailurePolicy(oldPolicy);
        testKiller.reset();
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Fail sync in CommitLog",
            targetClass = "CommitLog",
            targetMethod = "sync",
            condition = "org.apache.cassandra.db.commitlog.CommitLogPolicyBytemanTest.failSync",
            action = "throw new java.lang.RuntimeException(\"Fail CommitLog.sync to test fail_writes policy\");") } )
    public void testFailWritesPolicies()
    {
        DatabaseDescriptor.setCommitFailurePolicy(Config.CommitFailurePolicy.fail_writes);

        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(STANDARD);
        Mutation m = new RowUpdateBuilder(cfs.metadata.get(), 0, "key")
                .clustering("bytes")
                .add("val", ByteBuffer.allocate(10 * 1024))
                .build();

        CommitLog.instance.add(m);
        Assert.assertFalse(CommitLog.instance.shouldRejectMutations());

        failSync = true;
        await().atMost(2, TimeUnit.SECONDS)
                .until(() -> CommitLog.instance.shouldRejectMutations());
        Assert.assertThrows(FSWriteError.class, () -> CommitLog.instance.add(m));

        failSync = false;
        await().atMost(2, TimeUnit.SECONDS)
                .until(() -> !CommitLog.instance.shouldRejectMutations());
        CommitLog.instance.add(m);
    }
}
