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

import java.util.Collections;

import com.google.common.base.Objects;

import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

/**
 * An {@link SSTableContext} is created for an individual sstable and is shared across column indexes to track per-sstable
 * index files.
 * <p>
 * The {@link SSTableContext} will be released when receiving a sstable removed notification, but its shared copies in
 * individual {@link SSTableIndex}es will be released when in-flight read requests complete.
 */
public class SSTableContext extends SharedCloseableImpl
{
    public final SSTableReader sstable;
    public final IndexDescriptor indexDescriptor;
    public final PrimaryKeyMap.Factory primaryKeyMapFactory;

    private SSTableContext(SSTableReader sstable,
                           IndexDescriptor indexDescriptor,
                           PrimaryKeyMap.Factory primaryKeyMapFactory,
                           Cleanup cleanup)
    {
        super(cleanup);
        this.sstable = sstable;
        this.indexDescriptor = indexDescriptor;
        this.primaryKeyMapFactory = primaryKeyMapFactory;
    }

    private SSTableContext(SSTableContext copy)
    {
        super(copy);
        this.sstable = copy.sstable;
        this.indexDescriptor = copy.indexDescriptor;
        this.primaryKeyMapFactory = copy.primaryKeyMapFactory;
    }

    public static SSTableContext create(SSTableReader sstable)
    {
        Ref<? extends SSTableReader> sstableRef = null;
        PrimaryKeyMap.Factory primaryKeyMapFactory = null;

        IndexDescriptor indexDescriptor = IndexDescriptor.create(sstable);
        try
        {
            sstableRef = sstable.tryRef();

            if (sstableRef == null)
            {
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);
            }

            primaryKeyMapFactory = indexDescriptor.newPrimaryKeyMapFactory(sstable);

            Cleanup cleanup = new Cleanup(primaryKeyMapFactory, indexDescriptor, sstableRef);

            return new SSTableContext(sstable, indexDescriptor, primaryKeyMapFactory, cleanup);
        }
        catch (Throwable t)
        {
            if (sstableRef != null)
            {
                sstableRef.release();
            }

            throw Throwables.unchecked(Throwables.close(t, Collections.singleton(primaryKeyMapFactory)));
        }
    }

    @Override
    public SSTableContext sharedCopy()
    {
        return new SSTableContext(this);
    }

    /**
     * Returns a new {@link SSTableIndex} for a per-column index
     */
    public SSTableIndex newSSTableIndex(StorageAttachedIndex index)
    {
        return indexDescriptor.newSSTableIndex(this, index);
    }

    /**
     * @return descriptor of attached sstable
     */
    public Descriptor descriptor()
    {
        return sstable.descriptor;
    }

    /**
     * @return disk usage (in bytes) of per-sstable index files
     */
    public long diskUsage()
    {
        return indexDescriptor.sizeOnDiskOfPerSSTableComponents();
    }

    /**
     * @return number of open files per {@link SSTableContext} instance
     */
    public int openFilesPerSSTable()
    {
        return indexDescriptor.version.onDiskFormat().openFilesPerSSTableIndex(indexDescriptor.hasClustering());
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
        private final IndexDescriptor indexDescriptor;
        private final Ref<? extends SSTableReader> sstableRef;

        private Cleanup(PrimaryKeyMap.Factory primaryKeyMapFactory,
                        IndexDescriptor indexDescriptor,
                        Ref<? extends SSTableReader> sstableRef)
        {
            this.primaryKeyMapFactory = primaryKeyMapFactory;
            this.indexDescriptor = indexDescriptor;
            this.sstableRef = sstableRef;
        }

        @Override
        public void tidy()
        {
            Throwable t = sstableRef.ensureReleased(null);
            t = Throwables.close(t, Collections.singleton(primaryKeyMapFactory));

            Throwables.maybeFail(t);
        }

        @Override
        public String name()
        {
            return indexDescriptor.toString();
        }
    }
}
