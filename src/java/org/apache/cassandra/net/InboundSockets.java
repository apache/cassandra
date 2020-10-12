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
package org.apache.cassandra.net;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import io.netty.util.concurrent.PromiseNotifier;
import io.netty.util.concurrent.SucceededFuture;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;

class InboundSockets
{
    /**
     * A simple struct to wrap up the components needed for each listening socket.
     */
    @VisibleForTesting
    static class InboundSocket
    {
        public final InboundConnectionSettings settings;

        /**
         * The base {@link Channel} that is doing the socket listen/accept.
         * Null only until open() is invoked and {@link #binding} has yet to complete.
         */
        private volatile Channel listen;
        /**
         * Once open() is invoked, this holds the future result of opening the socket,
         * so that its completion can be waited on. Once complete, it sets itself to null.
         */
        private volatile ChannelFuture binding;

        // purely to prevent close racing with open
        private boolean closedWithoutOpening;

        // used to prevent racing on close
        private Future<Void> closeFuture;

        /**
         * A group of the open, inbound {@link Channel}s connected to this node. This is mostly interesting so that all of
         * the inbound connections/channels can be closed when the listening socket itself is being closed.
         */
        private final ChannelGroup connections;
        private final DefaultEventExecutor executor;

        private InboundSocket(InboundConnectionSettings settings)
        {
            this.settings = settings;
            this.executor = new DefaultEventExecutor(new NamedThreadFactory("Listen-" + settings.bindAddress));
            this.connections = new DefaultChannelGroup(settings.bindAddress.toString(), executor);
        }

        private Future<Void> open()
        {
            return open(pipeline -> {});
        }

        private Future<Void> open(Consumer<ChannelPipeline> pipelineInjector)
        {
            synchronized (this)
            {
                if (listen != null)
                    return new SucceededFuture<>(GlobalEventExecutor.INSTANCE, null);
                if (binding != null)
                    return binding;
                if (closedWithoutOpening)
                    throw new IllegalStateException();
                binding = InboundConnectionInitiator.bind(settings, connections, pipelineInjector);
            }

            return binding.addListener(ignore -> {
                synchronized (this)
                {
                    if (binding.isSuccess())
                        listen = binding.channel();
                    binding = null;
                }
            });
        }

        /**
         * Close this socket and any connections created on it. Once closed, this socket may not be re-opened.
         *
         * This may not execute synchronously, so a Future is returned encapsulating its result.
         * @param shutdownExecutors consumer invoked with the internal executor on completion
         *                          Note that the consumer will only be invoked once per InboundSocket.
         *                          Subsequent calls to close will not register a callback to different consumers.
         */
        private Future<Void> close(Consumer<? super ExecutorService> shutdownExecutors)
        {
            AsyncPromise<Void> done = AsyncPromise.uncancellable(GlobalEventExecutor.INSTANCE);

            Runnable close = () -> {
                List<Future<Void>> closing = new ArrayList<>();
                if (listen != null)
                    closing.add(listen.close());
                closing.add(connections.close());
                new FutureCombiner(closing)
                       .addListener(future -> {
                           executor.shutdownGracefully();
                           shutdownExecutors.accept(executor);
                       })
                       .addListener(new PromiseNotifier<>(done));
            };

            synchronized (this)
            {
                if (listen == null && binding == null)
                {
                    closedWithoutOpening = true;
                    return new SucceededFuture<>(GlobalEventExecutor.INSTANCE, null);
                }

                if (closeFuture != null)
                {
                    return closeFuture;
                }

                closeFuture = done;

                if (listen != null)
                {
                    close.run();
                }
                else
                {
                    binding.cancel(true);
                    binding.addListener(future -> close.run());
                }

                return done;
            }
        }

        public boolean isOpen()
        {
            return listen != null && listen.isOpen();
        }
    }

    private final List<InboundSocket> sockets;

    InboundSockets(InboundConnectionSettings template)
    {
        this(withDefaultBindAddresses(template));
    }

    InboundSockets(List<InboundConnectionSettings> templates)
    {
        this.sockets = bindings(templates);
    }

    private static List<InboundConnectionSettings> withDefaultBindAddresses(InboundConnectionSettings template)
    {
        ImmutableList.Builder<InboundConnectionSettings> templates = ImmutableList.builder();
        templates.add(template.withBindAddress(FBUtilities.getLocalAddressAndPort()));
        if (shouldListenOnBroadcastAddress())
            templates.add(template.withBindAddress(FBUtilities.getBroadcastAddressAndPort()));
        return templates.build();
    }

    private static List<InboundSocket> bindings(List<InboundConnectionSettings> templates)
    {
        ImmutableList.Builder<InboundSocket> sockets = ImmutableList.builder();
        for (InboundConnectionSettings template : templates)
            addBindings(template, sockets);
        return sockets.build();
    }

    private static void addBindings(InboundConnectionSettings template, ImmutableList.Builder<InboundSocket> out)
    {
        InboundConnectionSettings       settings = template.withDefaults();
        InboundConnectionSettings legacySettings = template.withLegacyDefaults();

        if (settings.encryption.enable_legacy_ssl_storage_port)
        {
            out.add(new InboundSocket(legacySettings));

            /*
             * If the legacy ssl storage port and storage port match, only bind to the
             * legacy ssl port. This makes it possible to configure a 4.0 node like a 3.0
             * node with only the ssl_storage_port if required.
             */
            if (settings.bindAddress.equals(legacySettings.bindAddress))
                return;
        }

        out.add(new InboundSocket(settings));
    }

    public Future<Void> open(Consumer<ChannelPipeline> pipelineInjector)
    {
        List<Future<Void>> opening = new ArrayList<>();
        for (InboundSocket socket : sockets)
            opening.add(socket.open(pipelineInjector));

        return new FutureCombiner(opening);
    }

    public Future<Void> open()
    {
        List<Future<Void>> opening = new ArrayList<>();
        for (InboundSocket socket : sockets)
            opening.add(socket.open());
        return new FutureCombiner(opening);
    }

    public boolean isListening()
    {
        for (InboundSocket socket : sockets)
            if (socket.isOpen())
                return true;
        return false;
    }

    public Future<Void> close(Consumer<? super ExecutorService> shutdownExecutors)
    {
        List<Future<Void>> closing = new ArrayList<>();
        for (InboundSocket address : sockets)
            closing.add(address.close(shutdownExecutors));
        return new FutureCombiner(closing);
    }
    public Future<Void> close()
    {
        return close(e -> {});
    }

    private static boolean shouldListenOnBroadcastAddress()
    {
        return DatabaseDescriptor.shouldListenOnBroadcastAddress()
               && !FBUtilities.getLocalAddressAndPort().equals(FBUtilities.getBroadcastAddressAndPort());
    }

    @VisibleForTesting
    public List<InboundSocket> sockets()
    {
        return sockets;
    }
}