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
package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.streaming.CassandraEntireSSTableStreamReader;
import org.apache.cassandra.db.streaming.CassandraIncomingFile;
import org.apache.cassandra.db.streaming.CassandraStreamManager;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.RangeAwareSSTableWriter;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.streaming.IncomingStream;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

/** This is a somewhat brittle test to demonstrate transaction log corruption
    when streaming is aborted as streamed sstables are added to the
    transaction log concurrently.

    The transaction log should not be modified after streaming
    has aborted or completed it.
*/
public class StreamFailedWhileReceivingTest extends TestBaseImpl
{
    @Test
    public void zeroCopy() throws IOException
    {
        streamClose(true);
    }

    @Test
    public void notZeroCopy() throws IOException
    {
        streamClose(false);
    }

    private void streamClose(boolean zeroCopyStreaming) throws IOException
    {
        try (Cluster cluster = Cluster.build(2)
                                      .withInstanceInitializer(BBHelper::install)
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set("stream_entire_sstables", zeroCopyStreaming)
                                                        .set("autocompaction_on_startup_enabled", false))
                                      .start())
        {
            init(cluster);

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int PRIMARY KEY)"));
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = cluster.get(2);
            for (int i = 1; i <= 100; i++)
                node1.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk) VALUES (?)"), i);
            node1.flush(KEYSPACE);

            // trigger streaming; expected to fail as streaming socket closed in the middle (currently this is an unrecoverable event)
            node2.nodetoolResult("repair", "-full", KEYSPACE, "tbl");

            node2.runOnInstance(() -> {
                try
                {
                    // use the startup logic to check for corrupt txn logfiles from the streaming failure
                    // quicker than restarting the instance to check
                    ColumnFamilyStore.scrubDataDirectories(Schema.instance.getTableMetadata(KEYSPACE, "tbl"));
                }
                catch (StartupException ex)
                {
                    throw new RuntimeException(ex);
                }
            });
        }
    }


    public static class BBHelper
    {
        static volatile StreamSession firstSession;
        static CountDownLatch firstStreamAbort = CountDownLatch.newCountDownLatch(1); // per-instance

        // CassandraStreamManager.prepareIncomingStream
        @SuppressWarnings("unused")
        public static IncomingStream prepareIncomingStream(StreamSession session, StreamMessageHeader header, @SuperCall Callable<IncomingStream> zuper) throws Exception
        {
            if (firstSession == null)
                firstSession = session;
            return zuper.call();
        }

        // CassandraStreamReceiver.abort
        @SuppressWarnings("unused")
        public static void abort(@SuperCall Callable<Integer> zuper) throws Exception
        {
            firstStreamAbort.decrement();
            zuper.call();
        }

        // RangeAwareSSTableWriter.append
        @SuppressWarnings("unused")
        public static boolean append(UnfilteredRowIterator partition, @SuperCall Callable<Boolean> zuper) throws Exception
        {
            // handles compressed and non-compressed
            if (isCaller(CassandraIncomingFile.class.getName(), "read"))
             {
                if (firstSession != null)
                {
                    firstSession.abort();
                    // delay here until CassandraStreamReceiver abort is called on NonPeriodic tasks
                    firstStreamAbort.awaitUninterruptibly(1, TimeUnit.MINUTES);
                }
             }
            return zuper.call();
        }

        // ColumnFamilyStore.createWriter - for entire sstable streaming, before adding to LogTransaction
        @SuppressWarnings("unused")
        public static Descriptor newSSTableDescriptor(File directory, Version version, SSTableFormat.Type format, @SuperCall Callable<Descriptor> zuper) throws Exception
        {
            if (isCaller(CassandraEntireSSTableStreamReader.class.getName(), "read"))
            // handles compressed and non-compressed
            {
                if (firstSession != null)
                {
                    firstSession.abort();
                    // delay here until CassandraStreamReceiver abort is called on NonPeriodic tasks
                    firstStreamAbort.awaitUninterruptibly(1, TimeUnit.MINUTES);
                }
            }
            return zuper.call();
        }

        private static boolean isCaller(String klass, String method)
        {
            StackTraceElement[] stack = Thread.currentThread().getStackTrace();
            for (int i = 0; i < stack.length; i++)
            {
                StackTraceElement e = stack[i];
                if (klass.equals(e.getClassName()) && method.equals(e.getMethodName()))
                    return true;
            }
            return false;
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != 2) // only target the second instance
                return;

            new ByteBuddy().rebase(CassandraStreamManager.class)
                           .method(named("prepareIncomingStream").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(RangeAwareSSTableWriter.class)
                           .method(named("append").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(ColumnFamilyStore.class)
                           .method(named("newSSTableDescriptor").and(takesArguments(3)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
            new ByteBuddy().rebase(CassandraStreamReceiver.class)
                           .method(named("abort").and(takesArguments(0)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }
    }
}
