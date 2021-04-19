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

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

import static org.apache.cassandra.index.sai.disk.OnDiskKeyProducer.NO_OFFSET;

/**
 * SSTableContext is created for individual sstable shared across indexes to track per-sstable index files.
 *
 * SSTableContext itself will be released when receiving sstable removed notification, but its shared copies in individual
 * SSTableIndex will be released when in-flight read requests complete.
 */
public class SSTableContext extends SharedCloseableImpl
{
    public final SSTableReader sstable;

    private final IndexComponents groupComponents;
    // mapping from sstable row id to token or offset
    public final LongArray.Factory tokenReaderFactory, offsetReaderFactory;
    public final KeyFetcher keyFetcher;

    private SSTableContext(SSTableReader sstable,
                           LongArray.Factory tokenReaderFactory,
                           LongArray.Factory offsetReaderFactory,
                           KeyFetcher keyFetcher,
                           Cleanup cleanup,
                           IndexComponents groupComponents)
    {
        super(cleanup);
        this.sstable = sstable;
        this.tokenReaderFactory = tokenReaderFactory;
        this.offsetReaderFactory = offsetReaderFactory;
        this.keyFetcher = keyFetcher;
        this.groupComponents = groupComponents;
    }

    private SSTableContext(SSTableContext copy)
    {
        super(copy);
        this.sstable = copy.sstable;
        this.tokenReaderFactory = copy.tokenReaderFactory;
        this.offsetReaderFactory = copy.offsetReaderFactory;
        this.groupComponents = copy.groupComponents;
        this.keyFetcher = copy.keyFetcher;
    }

    @SuppressWarnings("resource")
    public static SSTableContext create(SSTableReader sstable)
    {
        IndexComponents groupComponents = IndexComponents.perSSTable(sstable);

        Ref<? extends SSTableReader> sstableRef = null;
        FileHandle token = null, offset = null;
        LongArray.Factory tokenReaderFactory, offsetReaderFactory;
        KeyFetcher keyFetcher;
        try
        {
            MetadataSource source = MetadataSource.loadGroupMetadata(groupComponents);

            sstableRef = sstable.tryRef();

            if (sstableRef == null)
            {
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);
            }

            token = groupComponents.createFileHandle(IndexComponents.TOKEN_VALUES);
            offset  = groupComponents.createFileHandle(IndexComponents.OFFSETS_VALUES);

            tokenReaderFactory = new BlockPackedReader(token, IndexComponents.TOKEN_VALUES, groupComponents, source);
            offsetReaderFactory = new MonotonicBlockPackedReader(offset, IndexComponents.OFFSETS_VALUES, groupComponents, source);
            keyFetcher = new DecoratedKeyFetcher(sstable);

            Cleanup cleanup = new Cleanup(token, offset, sstableRef);

            return new SSTableContext(sstable, tokenReaderFactory, offsetReaderFactory, keyFetcher, cleanup, groupComponents);
        }
        catch (Throwable t)
        {
            if (sstableRef != null)
            {
                sstableRef.release();
            }

            throw Throwables.unchecked(Throwables.close(t, token, offset));
        }
    }

    /**
     * @return number of open files per {@link SSTableContext} instance
     */
    public static int openFilesPerSSTable()
    {
        // token and offset
        return 2;
    }

    @Override
    public SSTableContext sharedCopy()
    {
        return new SSTableContext(this);
    }

    private static class Cleanup implements RefCounted.Tidy
    {
        private final FileHandle token, offset;
        private final Ref<? extends SSTableReader> sstableRef;

        private Cleanup(FileHandle token, FileHandle offset, Ref<? extends SSTableReader> sstableRef)
        {
            this.token = token;
            this.offset = offset;
            this.sstableRef = sstableRef;
        }

        @Override
        public void tidy()
        {
            Throwable t = sstableRef.ensureReleased(null);
            t = Throwables.close(t, token, offset);

            Throwables.maybeFail(t);
        }

        @Override
        public String name()
        {
            return null;
        }
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

    /**
     * @return disk usage of per-sstable index files
     */
    public long diskUsage()
    {
        return groupComponents.sizeOfPerSSTableComponents();
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

    public interface KeyFetcher
    {
        DecoratedKey apply(RandomAccessReader reader, long keyOffset);

        /**
         * Create a shared RAR for all tokens in the same segment.
         */
        RandomAccessReader createReader();
    }

    @VisibleForTesting
    public static class DecoratedKeyFetcher implements KeyFetcher
    {
        private final SSTableReader sstable;

        DecoratedKeyFetcher(SSTableReader sstable)
        {
            this.sstable = sstable;
        }

        @Override
        public RandomAccessReader createReader()
        {
            return sstable.openKeyComponentReader();
        }

        @Override
        public DecoratedKey apply(RandomAccessReader reader, long keyOffset)
        {
            assert reader != null : "RandomAccessReader null";

            // If the returned offset is the sentinel value, we've seen this offset
            // before or we've run out of valid keys due to ZCS:
            if (keyOffset == NO_OFFSET)
                return null;

            try
            {
                // can return null
                return sstable.keyAt(reader, keyOffset);
            }
            catch (IOException e)
            {
                throw Throwables.cleaned(e);
            }
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this).add("sstable", sstable).toString();
        }

        @Override
        public int hashCode()
        {
            return sstable.descriptor.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == null)
            {
                return false;
            }
            if (other == this)
            {
                return true;
            }
            if (other.getClass() != getClass())
            {
                return false;
            }
            DecoratedKeyFetcher rhs = (DecoratedKeyFetcher) other;
            return sstable.descriptor.equals(rhs.sstable.descriptor);
        }
    }
}
