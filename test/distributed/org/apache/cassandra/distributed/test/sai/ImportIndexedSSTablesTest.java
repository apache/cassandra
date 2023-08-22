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

package org.apache.cassandra.distributed.test.sai;

import java.io.IOException;
import java.util.concurrent.Callable;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.StorageAttachedIndexBuilder;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class ImportIndexedSSTablesTest extends TestBaseImpl
{
    private static Cluster cluster;

    @BeforeClass
    public static void startup() throws IOException
    {
        cluster = init(Cluster.build(2).withConfig(c -> c.with(NETWORK, GOSSIP))
                                       .withInstanceInitializer(ByteBuddyHelper::installErrors)
                                       .start());

        cluster.disableAutoCompaction(KEYSPACE);
    }

    @AfterClass
    public static void shutdown()
    {
        if (cluster != null)
            cluster.close();
    }

    @Test
    public void testIndexBuildingFailureDuringImport()
    {
        String table = "fail_during_import_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = true);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() ->
                assertThatThrownBy(() ->
                        ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)).hasRootCauseExactlyInstanceOf(CompactionInterruptedException.class));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);
    }

    @Test
    public void testImportBuildsSSTableIndexes()
    {
        String table = "import_build_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);
    }

    @Test
    public void testValidationFailureDuringImport()
    {
        String table = "validation_failure_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = true);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() ->
                assertThatThrownBy(() ->
                        ColumnFamilyStore.loadNewSSTables(KEYSPACE, table)).hasRootCauseExactlyInstanceOf(CorruptIndexException.class));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);
    }

    @Test
    public void testImportIncludesExistingSSTableIndexes()
    {
        String table = "existing_indexes_test";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));

        IInvokableInstance first = cluster.get(1);
        first.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        first.runOnInstance(()-> ByteBuddyHelper.interruptBuild = false);

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);

        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        first.runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).clearUnsafe());

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);
        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        first.runOnInstance(() -> ColumnFamilyStore.loadNewSSTables(KEYSPACE, table));

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);

        rs = first.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
        assertThat(rs[0][0]).isEqualTo(1);
    }

    public static class ByteBuddyHelper
    {
        static volatile boolean interruptBuild = false;
        static volatile boolean failValidation = false;

        static void installErrors(ClassLoader loader, int node)
        {
            new ByteBuddy().rebase(StorageAttachedIndexBuilder.class)
                           .method(named("isStopRequested"))
                           .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(SAICodecUtils.class)
                           .method(named("validateChecksum"))
                           .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                           .make()
                           .load(loader, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static boolean isStopRequested(@SuperCall Callable<Boolean> zuper)
        {
            if (interruptBuild)
                return true;

            try
            {
                return zuper.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }

        @SuppressWarnings("unused")
        public static void validateChecksum(IndexInput input, @SuperCall Callable<Void> zuper) throws IOException
        {
            if (failValidation)
                throw new CorruptIndexException("Injected failure!", "Test resource");

            try
            {
                zuper.call();
            }
            catch (Exception e)
            {
                throw Throwables.unchecked(e);
            }
        }
    }
}
