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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SAICodecUtils;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class IndexStreamingFailureTest extends TestBaseImpl
{
    public static final String TEST_ERROR_MESSAGE = "Injected failure!";
    
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
    public void testAvailabilityAfterFailedNonEntireFileStreaming() throws Exception
    {
        cluster.get(2).runOnInstance(()-> ByteBuddyHelper.failFlush = true);
        cluster.get(2).runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        testAvailabilityAfterStreaming("non_entire_file_test", false);
    }

    @Test
    public void testAvailabilityAfterFailedEntireFileStreaming() throws Exception
    {
        cluster.get(2).runOnInstance(()-> ByteBuddyHelper.failFlush = false);
        cluster.get(2).runOnInstance(()-> ByteBuddyHelper.failValidation = true);
        testAvailabilityAfterStreaming("entire_file_test", true);
    }

    private void testAvailabilityAfterStreaming(String table, boolean streamEntireSSTables) throws Exception
    {
        String indexName = table + "_v_index";
        cluster.schemaChange(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v text)", KEYSPACE, table));
        cluster.schemaChange(String.format("CREATE INDEX %s ON %s.%s(v) USING 'sai'", indexName, KEYSPACE, table));
        SAIUtil.waitForIndexQueryable(cluster, KEYSPACE, indexName);

        IInvokableInstance first = cluster.get(1);
        IInvokableInstance second = cluster.get(2);
        first.runOnInstance(()-> DatabaseDescriptor.setStreamEntireSSTables(streamEntireSSTables));
        second.runOnInstance(()-> DatabaseDescriptor.setStreamEntireSSTables(streamEntireSSTables));

        first.executeInternal(String.format("INSERT INTO %s.%s(pk, v) VALUES (?, ?)", KEYSPACE, table), 1, "v1");
        first.flush(KEYSPACE);

        Object[][] rs = second.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        // The repair job should fail when index completion fails. This should also fail the streaming transaction.
        long mark = second.logs().mark();
        second.nodetoolResult("repair", KEYSPACE).asserts().failure();
        assertFalse("There should be an injected failure in the logs.", second.logs().grep(mark, TEST_ERROR_MESSAGE).getResult().isEmpty());

        // The SSTable should not be added to the table view, as the streaming transaction failed...
        rs = second.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);

        // ...and querying the index also returns nothing, as the index for the streamed SSTable was never built.
        rs = second.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        // On restart, ensure that the index remains querable and does not include the data we attempted to stream. 
        second.shutdown().get();
        second.startup();

        // On restart, the base table should be unchanged...
        rs = second.executeInternal(String.format("SELECT pk FROM %s.%s WHERE pk = ?", KEYSPACE, table), 1);
        assertThat(rs.length).isEqualTo(0);

        // ...and the index should remain queryable, because from its perspective, the streaming never happened.
        rs = second.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(0);

        // Disable failure injection, and verify that the index is queryable and has the newly streamed data:
        second.runOnInstance(()-> ByteBuddyHelper.failFlush = false);
        second.runOnInstance(()-> ByteBuddyHelper.failValidation = false);
        second.nodetoolResult("repair", KEYSPACE).asserts().success();

        rs = second.executeInternal(String.format("SELECT pk FROM %s.%s WHERE v = ?", KEYSPACE, table), "v1");
        assertThat(rs.length).isEqualTo(1);
    }

    public static class ByteBuddyHelper
    {
        volatile static boolean failFlush = false;
        volatile static boolean failValidation = false;
        
        @SuppressWarnings("resource")
        static void installErrors(ClassLoader loader, int node)
        {
            if (node == 2)
            {
                new ByteBuddy().rebase(SegmentBuilder.class)
                               .method(named("flush"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(loader, ClassLoadingStrategy.Default.INJECTION);

                new ByteBuddy().rebase(SAICodecUtils.class)
                               .method(named("validateChecksum"))
                               .intercept(MethodDelegation.to(ByteBuddyHelper.class))
                               .make()
                               .load(loader, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static SegmentMetadata flush(IndexDescriptor indexDescriptor, IndexContext indexContext, @SuperCall Callable<SegmentMetadata> zuper) throws IOException
        {
            if (failFlush)
                throw new IOException(TEST_ERROR_MESSAGE);

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
                throw new CorruptIndexException(TEST_ERROR_MESSAGE, "Test resource");

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
