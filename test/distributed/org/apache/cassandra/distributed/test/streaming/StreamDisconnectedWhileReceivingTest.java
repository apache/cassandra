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

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.messages.IncomingStreamMessage;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.STREAM;
import static org.junit.Assert.assertFalse;

/** Demonstrate deadlock if the netty event loop thread calls StreamSession.closeSession while
 *  the object monitor is being held by another thread.
*/
public class StreamDisconnectedWhileReceivingTest extends TestBaseImpl
{
    static final Logger logger = LoggerFactory.getLogger(StreamDisconnectedWhileReceivingTest.class);

    @Test
    public void zeroCopy() throws IOException, InterruptedException
    {
        disconnectControlChannel(true);
    }

    @Test
    public void notZeroCopy() throws IOException, InterruptedException
    {
        disconnectControlChannel(false);
    }

    private void disconnectControlChannel(boolean zeroCopyStreaming) throws IOException, InterruptedException
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
            Thread nodetoolRepair = new Thread(() -> {
                node2.nodetoolResult("repair", "-full", KEYSPACE, "tbl");
            });
            nodetoolRepair.start();

            nodetoolRepair.join(15000);
            assertFalse("Repair did not complete - assuming deadlock", nodetoolRepair.isAlive());

            // if deadlock occurs, instance shutdown will hit timeout.
        }
    }

    public static class BBHelper
    {
        static Logger logger = LoggerFactory.getLogger(BBHelper.class);

        public static void receive(IncomingStreamMessage message, @SuperCall Callable<Void> zuper) throws Throwable
        {
            logger.info("receive message {}", message.type);
            if (message.type == STREAM)
                message.stream.session().getChannel().unsafeCloseControlChannel();
            zuper.call();
        }

        public static void install(ClassLoader classLoader, Integer num)
        {
            if (num != 2) // only target the second instance
                return;

            new ByteBuddy().rebase(StreamSession.class)
                           .method(named("receive").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }
    }
}
