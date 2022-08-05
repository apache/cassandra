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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.function.IntFunction;

import io.netty.util.concurrent.Future; //checkstyle: permit this import
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION, inner = INTERFACES)
public interface StreamingChannel
{
    public interface Factory
    {
        public static class Global
        {
            private static StreamingChannel.Factory FACTORY = new NettyStreamingConnectionFactory();
            public static StreamingChannel.Factory streamingFactory()
            {
                return FACTORY;
            }

            public static void unsafeSet(StreamingChannel.Factory factory)
            {
                FACTORY = factory;
            }
        }

        StreamingChannel create(InetSocketAddress to, int messagingVersion, Kind kind) throws IOException;

        default StreamingChannel create(InetSocketAddress to,
                                        InetSocketAddress preferred,
                                        int messagingVersion,
                                        StreamingChannel.Kind kind) throws IOException
        {
            // Implementations can decide whether or not to do something with the preferred address.
            return create(to, messagingVersion, kind);
        }

        /** Provide way to disable getPreferredIP() for tools without access to the system keyspace
         *
         * CASSANDRA-17663 moves calls to SystemKeyspace.getPreferredIP() outside of any threads
         * that are regularly interrupted.  However the streaming subsystem is also used
         * by the bulk loader tool, which does not have direct access to the local tables
         * and uses the client metadata/queries to retrieve it.
         *
         * @return true if SystemKeyspace.getPreferredIP() should be used when connecting
         */
        default boolean supportsPreferredIp()
        {
            return true;
        }
    }

    public enum Kind { CONTROL, FILE }

    public interface Send
    {
        void send(IntFunction<StreamingDataOutputPlus> outSupplier) throws IOException;
    }

    Object id();
    String description();

    InetSocketAddress peer();
    InetSocketAddress connectedTo();
    boolean connected();

    StreamingDataInputPlus in();

    /**
     * until closed, cannot invoke {@link #send(Send)}
     */
    StreamingDataOutputPlus acquireOut();
    Future<?> send(Send send) throws IOException;

    Future<?> close();
    void onClose(Runnable runOnClose);
}
