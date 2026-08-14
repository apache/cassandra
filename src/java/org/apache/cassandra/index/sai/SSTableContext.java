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
package org.apache.cassandra.index.sai;

import com.google.common.base.Objects;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

/**
 * SSTableContext is created for individual sstable shared across indexes to track per-sstable index files.
 *
 * SSTableContext itself will be released when receiving sstable removed notification, but its shared copies in individual
 * SSTableIndex will be released when in-flight read requests complete.
 */
public class SSTableContext extends SharedCloseableImpl
{
    public final SSTableReader sstable;
    private final IndexComponents.ForRead perSSTableComponents;
    public final PrimaryKey.Factory primaryKeyFactory;
    public final PrimaryKeyMap.Factory primaryKeyMapFactory;

    private SSTableContext(SSTableReader sstable,
                           IndexComponents.ForRead perSSTableComponents,
                           PrimaryKey.Factory primaryKeyFactory,
                           PrimaryKeyMap.Factory primaryKeyMapFactory,
                           Cleanup cleanup)
    {
        super(cleanup);
        this.sstable = sstable;
        this.perSSTableComponents = perSSTableComponents;
        this.primaryKeyFactory = primaryKeyFactory;
        this.primaryKeyMapFactory = primaryKeyMapFactory;
    }

    private SSTableContext(SSTableContext copy)
    {
        super(copy);
        this.sstable = copy.sstable;
        this.perSSTableComponents = copy.perSSTableComponents;
        this.primaryKeyFactory = copy.primaryKeyFactory;
        this.primaryKeyMapFactory = copy.primaryKeyMapFactory;
    }

    public static SSTableContext create(SSTableReader sstable, IndexComponents.ForRead perSSTableComponents)
    {
        var onDiskFormat = perSSTableComponents.onDiskFormat();
        // no disk access thus no need to use EmptyFactory
        PrimaryKey.Factory primaryKeyFactory = onDiskFormat.newPrimaryKeyFactory(sstable.metadata().comparator);

        Ref<? extends SSTableReader> sstableRef = null;
        PrimaryKeyMap.Factory primaryKeyMapFactory = null;

        try
        {
            sstableRef = sstable.tryRef();

            if (sstableRef == null)
            {
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);
            }

            // avoid opening SAI metadata if reads are disabled
            primaryKeyMapFactory = CassandraRelevantProperties.SAI_INDEX_READS_DISABLED.getBoolean()
                                   ? new PrimaryKeyMap.DummyThrowingFactory()
                                   : onDiskFormat.newPrimaryKeyMapFactory(perSSTableComponents, primaryKeyFactory, sstable);

            Cleanup cleanup = new Cleanup(primaryKeyMapFactory, sstableRef);

            return new SSTableContext(sstable, perSSTableComponents, primaryKeyFactory, primaryKeyMapFactory, cleanup);
        }
        catch (Throwable t)
        {
            if (sstableRef != null)
            {
                sstableRef.release();
            }

            throw Throwables.unchecked(Throwables.close(t, primaryKeyMapFactory));
        }
    }

    /**
     * Returns the concrete on-disk perSStable components used by this context instance.
     */
    public IndexComponents.ForRead usedPerSSTableComponents()
    {
        return perSSTableComponents;
    }

    @Override
    public SSTableContext sharedCopy()
    {
        return new SSTableContext(this);
    }

    /**
     * @return descriptor of attached sstable
     */
    public Descriptor descriptor()
    {
        return sstable.descriptor;
    }

    public SSTableReader sstable()
    {
        return sstable;
    }

    public PrimaryKey.Factory primaryKeyFactory()
    {
        return primaryKeyFactory;
    }

    public PrimaryKeyMap.Factory primaryKeyMapFactory()
    {
        return primaryKeyMapFactory;
    }

    /**
     * @return number of open files per {@link SSTableContext} instance
     */
    public int openFilesPerSSTable()
    {
        return perSSTableComponents.onDiskFormat()
                                   .openFilesPerSSTable(sstable.metadata().hasClustering());
    }

    @Override
    public String toString()
    {
        return "SSTableContext{" +
               "sstable=" + sstable.descriptor +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableContext that = (SSTableContext) o;
        return Objects.equal(sstable.descriptor, that.sstable.descriptor);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sstable.descriptor.hashCode());
    }

    private static class Cleanup implements RefCounted.Tidy
    {
        private final PrimaryKeyMap.Factory primaryKeyMapFactory;
        private final Ref<? extends SSTableReader> sstableRef;

        private Cleanup(PrimaryKeyMap.Factory primaryKeyMapFactory, Ref<? extends SSTableReader> sstableRef)
        {
            this.primaryKeyMapFactory = primaryKeyMapFactory;
            this.sstableRef = sstableRef;
        }

        @Override
        public void tidy()
        {
            Throwable t = sstableRef.ensureReleased(null);
            t = Throwables.close(t, primaryKeyMapFactory);

            Throwables.maybeFail(t);
        }

        @Override
        public String name()
        {
            return null;
        }
    }
}
