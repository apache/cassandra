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

import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.Uninterruptibles;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SSTableImporter;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.io.sstable.Descriptor;

import static java.lang.String.format;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.assertj.core.api.Assertions.assertThat;

public class SSTableImporterConcurrencyTest extends TestBaseImpl
{
    @Test
    public void testAnImportDoesNotWaitForAnotherToVerify() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(1).withInstanceInitializer(BB::install).start()))
        {
            IInvokableInstance node = cluster.get(1);
            cluster.schemaChange(format("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)", KEYSPACE));
            String slow = Files.createTempDirectory("slow").toString();
            String fast = Files.createTempDirectory("fast").toString();

            for (String directory : new String[]{ slow, fast })
            {
                try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                               .forTable(format("CREATE TABLE %s.tbl (k int PRIMARY KEY, v int)", KEYSPACE))
                                                               .inDirectory(directory)
                                                               .using(format("INSERT INTO %s.tbl (k, v) VALUES (?, ?)", KEYSPACE))
                                                               .build())
                {
                    writer.addRow(0, 0);
                }
            }

            Future<List<String>> slowImport = node.asyncCallsOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").importNewSSTables(Collections.singleton(slow), false, false, true, false, false, false)).call();
            node.runOnInstance(() -> Uninterruptibles.awaitUninterruptibly(BB.held));
            assertThat(node.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").importNewSSTables(Collections.singleton(fast), false, false, true, false, false, false))).isEmpty();
            assertThat(slowImport.isDone()).isFalse();
            node.runOnInstance(() -> BB.released.countDown());
            assertThat(slowImport.get()).isEmpty();
            assertThat(node.callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().size())).isEqualTo(2);
        }
    }

    public static class BB
    {
        static final CountDownLatch held = new CountDownLatch(1);
        static final CountDownLatch released = new CountDownLatch(1);

        static void install(ClassLoader cl, int node)
        {
            new ByteBuddy().rebase(SSTableImporter.class)
                           .method(named("verifySSTableForImport"))
                           .intercept(MethodDelegation.to(BB.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void verifySSTableForImport(Descriptor descriptor, @SuperCall Callable<?> verify) throws Exception
        {
            if (descriptor.directory.name().startsWith("slow"))
            {
                held.countDown();
                released.await();
            }
            verify.call();
        }
    }
}
