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

package org.apache.cassandra.service.storage;

import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.INotificationConsumer;

/**
 * Creates the {@link ChannelProxy} through which sstable components are read.
 * <p>
 * This is the single point at which Cassandra decides <em>how</em> to obtain the bytes of a file, as opposed to
 * where that file lives. Implementations may back a channel with something other than the local filesystem, which
 * is why {@link org.apache.cassandra.io.util.FileHandle.Builder} asks for one rather than constructing a
 * {@link ChannelProxy} directly.
 * <p>
 * Implementations must be thread-safe: a single factory serves every sstable opened by the node.
 */
public interface ChannelProxyFactory
{
    /**
     * Opens the file on the local filesystem, which is the behaviour of a node with no storage provider
     * configured. It is the default so that a missing or failed provider degrades to the local path rather than
     * leaving the node unable to read its own sstables.
     */
    ChannelProxyFactory LOCAL = new ChannelProxyFactory()
    {
        public ChannelProxy create(File file, ChannelProxy.IOMode ioMode) { return new ChannelProxy(file, ioMode); }
        public void configure(Map<String, String> options) { }
    };

    /**
     * @param file   the file to open
     * @param ioMode buffered or direct I/O, as resolved from {@code disk_access_mode}
     * @return a new channel over {@code file}; the caller owns it and must release it
     */
    ChannelProxy create(File file, ChannelProxy.IOMode ioMode);

    /**
     * Applies provider-specific options from cassandra.yaml. Defaulted so a provider with nothing to
     * configure - the local one included - stays expressible as a lambda, and so later additions to this
     * interface do not break implementations compiled against an older version of it.
     */
    default void configure(Map<String, String> options) { }

    /**
     * A consumer of sstable lifecycle notifications, for a provider that has to act when an sstable appears or is
     * dropped - copying it to remote storage, say, and deleting the copy again afterwards.
     * <p>
     * Needed because {@link #create} alone cannot express that: it is asked for a channel over a file that
     * already exists, long after the decision to store that file somewhere has been made. Subscribed per table
     * by {@link org.apache.cassandra.db.ColumnFamilyStore}, so it is called on flush and compaction threads and
     * must not block them.
     *
     * @return the consumer to subscribe, or empty for a provider that only reads
     */
    default Optional<INotificationConsumer> notificationConsumer()
    {
        return Optional.empty();
    }

    /**
     * Releases whatever the provider holds - connection pools, executors, work still in flight.
     * <p>
     * Called from the drain path after the last flush, so a provider doing asynchronous work on behalf of
     * {@link #notificationConsumer} gets the chance to finish it rather than having it abandoned when the JVM
     * exits. Implementations should bound how long they block.
     */
    default void shutdown() { }
}
