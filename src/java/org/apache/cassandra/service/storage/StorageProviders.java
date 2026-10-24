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

import java.net.URL;
import java.util.Arrays;
import java.util.Optional;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.INotificationConsumer;


public class StorageProviders
{
    private static final Logger logger = LoggerFactory.getLogger(StorageProviders.class);

    private static final ChannelProxyFactory LOCAL = ChannelProxyFactory.LOCAL;
    private static volatile ChannelProxyFactory PLUGIN = null;

    public static void initialize()
    {
        StorageProviderConfig cfg = DatabaseDescriptor.getStorageProviderConfig();
        if (cfg == null) return;

        File dir = new File(cfg.pluginDir); // e.g. lib/providers/s3
        URL[] jars = Arrays.stream(dir.tryList(f -> f.name().endsWith(".jar")))
                           .map(f -> {
                               try
                               {
                                   return f.toJavaIOFile().toURL();
                               }
                               catch (Throwable t)
                               {
                                   throw new RuntimeException(String.format("Invalid file %s to create URL for.",
                                                                            f.toString()));
                               }
                           })
                           .toArray(URL[]::new);

        ClassLoader pluginLoader = new PluginClassLoader(jars, StorageProviders.class.getClassLoader());

        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try
        {
            Thread.currentThread().setContextClassLoader(pluginLoader);
            PLUGIN = ServiceLoader.load(ChannelProxyFactory.class, pluginLoader)
                                  .findFirst()
                                  .orElseThrow(() -> new ConfigurationException("No ChannelProxyFactory found in " + dir));

            PLUGIN.configure(cfg.options);
        }
        finally
        {
            Thread.currentThread().setContextClassLoader(saved);
        }

        logger.info("Storage provider {} loaded from {}", PLUGIN.getClass().getName(), dir);
    }

    public static ChannelProxyFactory factory()
    {
        return PLUGIN == null ? ChannelProxyFactory.LOCAL : PLUGIN;
    }

    /**
     * Replaces a live reader with one opened afresh over the same descriptor, so that a component the provider
     * has just taken ownership of stops being served from local disk.
     * <p>
     * Needed because unlinking a component does not release it. The reader opened when the sstable was added
     * still holds a descriptor on the file, and the kernel keeps that descriptor working - so reads carry on
     * from the unlinked inode and, more to the point, its blocks stay allocated. The space only comes back when
     * the last reference goes, and a reader opened now has no local file to find and resolves the component
     * through the provider instead.
     * <p>
     * The same swap, over the same files, is what {@code IndexSummaryRedistribution} performs when it resamples
     * an index summary, which is where the shape of this comes from.
     *
     * @return true if the reader was replaced
     */
    public static boolean reopen(SSTableReader sstable)
    {
        ColumnFamilyStore cfs = Keyspace.open(sstable.metadata().keyspace)
                                        .getColumnFamilyStore(sstable.metadata().id);

        // Null when something else already holds the sstable - a compaction, most likely. That operation either
        // obsoletes it, which frees the blocks anyway, or releases it, and the next flush brings us back here.
        try (LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UNKNOWN))
        {
            if (txn == null)
            {
                logger.debug("Not reopening {}, it is held by another operation", sstable.descriptor);
                return false;
            }

            SSTableReader reopened = SSTableReader.open(cfs,
                                                        sstable.descriptor,
                                                        sstable.getComponents(),
                                                        cfs.metadata);
            txn.update(reopened, true);
            txn.finish();

            logger.debug("Reopened {} through the storage provider", sstable.descriptor);
            return true;
        }
    }

    /**
     * The length of a file the configured provider may be holding outside the local filesystem.
     * <p>
     * {@code File.length()} answers this for a local file, but it is a stat() and so reports 0 for a component
     * the provider has offloaded - which makes an intact sstable look empty. Call sites that need the real
     * length of an sstable component should come through here rather than stat the path directly.
     */
    public static long length(File file)
    {
        if (DatabaseDescriptor.getStorageProviderConfig() == null || file.exists())
            return file.length();

        try (ChannelProxy channel = factory().create(file, ChannelProxy.IOMode.BUFFERED))
        {
            return channel.size();
        }
    }

    /**
     * The lifecycle consumer of the configured provider, if it has one, for a table's tracker to subscribe.
     * Empty for the local provider and for any provider that only reads.
     */
    public static Optional<INotificationConsumer> notificationConsumer()
    {
        return factory().notificationConsumer();
    }

    /**
     * Shuts the configured provider down. A failure here must not stop a drain, which is why it is logged rather
     * than propagated: by this point the data is already safe on local disk.
     */
    public static void shutdown()
    {
        try
        {
            factory().shutdown();
        }
        catch (Throwable t)
        {
            logger.warn("Storage provider {} failed to shut down cleanly", factory().getClass().getName(), t);
        }
    }
}
