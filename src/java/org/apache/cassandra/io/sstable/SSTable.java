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
package org.apache.cassandra.io.sstable;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.IOOptions;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.format.big.RowIndexEntry;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.memory.HeapCloner;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.service.ActiveRepairService.NO_PENDING_REPAIR;
import static org.apache.cassandra.service.ActiveRepairService.UNREPAIRED_SSTABLE;

/**
 * This class is built on top of the SequenceFile. It stores
 * data on disk in sorted fashion. However the sorting is upto
 * the application. This class expects keys to be handed to it
 * in sorted order.
 * <p>
 * A separate index file is maintained as well, containing the
 * SSTable keys and the offset into the SSTable at which they are found.
 * Every 1/indexInterval key is read into memory when the SSTable is opened.
 * <p>
 * Finally, a bloom filter file is also kept for the keys in each SSTable.
 */
public abstract class SSTable
{
    static final Logger logger = LoggerFactory.getLogger(SSTable.class);

    public static final int TOMBSTONE_HISTOGRAM_BIN_SIZE = 100;
    public static final int TOMBSTONE_HISTOGRAM_SPOOL_SIZE = 100000;
    public static final int TOMBSTONE_HISTOGRAM_TTL_ROUND_SECONDS = Integer.valueOf(System.getProperty("cassandra.streaminghistogram.roundseconds", "60"));

    public final Descriptor descriptor;
    protected final Set<Component> components;
    public final boolean compression;

    public DecoratedKey first;
    public DecoratedKey last;

    protected final TableMetadataRef metadata;

    protected final ChunkCache chunkCache;
    protected final IOOptions ioOptions;

    public SSTable(SSTableBuilder<?, ?> builder)
    {
        checkNotNull(builder.descriptor);
        checkNotNull(builder.getComponents());

        this.descriptor = builder.descriptor;
        this.ioOptions = builder.getIOOptions();
        this.components = new CopyOnWriteArraySet<>(builder.getComponents());
        this.compression = components.contains(Component.COMPRESSION_INFO);
        this.metadata = builder.getTableMetadataRef();
        this.chunkCache = builder.getChunkCache();
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        components.stream()
                  .filter(c -> !newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .filter(c -> !c.equals(Component.DATA))
                  .forEach(c -> tmpdesc.fileFor(c).move(newdesc.fileFor(c)));

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        tmpdesc.fileFor(Component.DATA).move(newdesc.fileFor(Component.DATA));

        // rename it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        components.stream()
                  .filter(c -> newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .forEach(c -> tmpdesc.fileFor(c).tryMove(newdesc.fileFor(c)));
    }

    public static void copy(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        components.stream()
                  .filter(c -> !newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .filter(c -> !c.equals(Component.DATA))
                  .forEach(c -> FileUtils.copyWithConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.copyWithConfirm(tmpdesc.fileFor(Component.DATA), newdesc.fileFor(Component.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        components.stream()
                  .filter(c -> newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .forEach(c -> FileUtils.copyWithOutConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));
    }

    public static void hardlink(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        components.stream()
                  .filter(c -> !newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .filter(c -> !c.equals(Component.DATA))
                  .forEach(c -> FileUtils.createHardLinkWithConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));

        // do -Data last because -Data present should mean the sstable was completely copied before crash
        FileUtils.createHardLinkWithConfirm(tmpdesc.fileFor(Component.DATA), newdesc.fileFor(Component.DATA));

        // copy it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        components.stream()
                  .filter(c -> newdesc.getFormat().generatedOnLoadComponents().contains(c))
                  .forEach(c -> FileUtils.createHardLinkWithoutConfirm(tmpdesc.fileFor(c), newdesc.fileFor(c)));
    }

    @VisibleForTesting
    public Set<Component> getComponents()
    {
        return ImmutableSet.copyOf(components);
    }

    /**
     * We use a ReferenceQueue to manage deleting files that have been compacted
     * and for which no more SSTable references exist.  But this is not guaranteed
     * to run for each such file because of the semantics of the JVM gc.  So,
     * we write a marker to `compactedFilename` when a file is compacted;
     * if such a marker exists on startup, the file should be removed.
     * <p>
     * This method will also remove SSTables that are marked as temporary.
     *
     * @return true if the file was deleted
     */
    public static boolean delete(Descriptor desc, Set<Component> components)
    {
        logger.info("Deleting sstable: {}", desc);
        // remove the DATA component first if it exists
        if (components.contains(Component.DATA))
            FileUtils.deleteWithConfirm(desc.filenameFor(Component.DATA));
        for (Component component : components)
        {
            if (component.equals(Component.DATA) || component.equals(Component.SUMMARY))
                continue;

            FileUtils.deleteWithConfirm(desc.filenameFor(component));
        }

        if (components.contains(Component.SUMMARY))
            FileUtils.delete(desc.filenameFor(Component.SUMMARY));

        return true;
    }

    public TableMetadata metadata()
    {
        return metadata.get();
    }

    public IPartitioner getPartitioner()
    {
        return metadata().partitioner;
    }

    public DecoratedKey decorateKey(ByteBuffer key)
    {
        return getPartitioner().decorateKey(key);
    }

    /**
     * If the given @param key occupies only part of a larger buffer, allocate a new buffer that is only
     * as large as necessary.
     */
    public static DecoratedKey getMinimalKey(DecoratedKey key)
    {
        return ByteBufferUtil.canMinimize(key.getKey())
               ? new BufferDecoratedKey(key.getToken(), HeapCloner.instance.clone(key.getKey()))
               : key;
    }

    public String getFilename()
    {
        return descriptor.filenameFor(Component.DATA);
    }

    public String getColumnFamilyName()
    {
        return descriptor.cfname;
    }

    public String getKeyspaceName()
    {
        return descriptor.ksname;
    }

    public List<String> getAllFilePaths()
    {
        List<String> ret = new ArrayList<>(components.size());
        for (Component component : components)
            ret.add(descriptor.filenameFor(component));
        return ret;
    }

    protected <B extends SSTableBuilder<?, B>> B unbuildTo(B builder)
    {
        return builder.setTableMetadataRef(metadata)
                      .setComponents(components)
                      .setChunkCache(chunkCache)
                      .setIOOptions(ioOptions);
    }

    /**
     * Parse a sstable filename into both a {@link Descriptor} and {@code Component} object.
     *
     * @param file the filename to parse.
     * @return a pair of the {@code Descriptor} and {@code Component} corresponding to {@code file} if it corresponds to
     * a valid and supported sstable filename, {@code null} otherwise. Note that components of an unknown type will be
     * returned as CUSTOM ones.
     */
    public static Pair<Descriptor, Component> tryComponentFromFilename(File file)
    {
        try
        {
            return Descriptor.fromFilenameWithComponent(file);
        }
        catch (Throwable e)
        {
            return null;
        }
    }

    /**
     * Parse a sstable filename into both a {@link Descriptor} and {@code Component} object.
     *
     * @param file     the filename to parse.
     * @param keyspace The keyspace name of the file.
     * @param table    The table name of the file.
     * @return a pair of the {@code Descriptor} and {@code Component} corresponding to {@code file} if it corresponds to
     * a valid and supported sstable filename, {@code null} otherwise. Note that components of an unknown type will be
     * returned as CUSTOM ones.
     */
    public static Pair<Descriptor, Component> tryComponentFromFilename(File file, String keyspace, String table)
    {
        try
        {
            return Descriptor.fromFilenameWithComponent(file, keyspace, table);
        }
        catch (Throwable e)
        {
            return null;
        }
    }

    /**
     * Parse a sstable filename into a {@link Descriptor} object.
     * <p>
     * Note that this method ignores the component part of the filename; if this is not what you want, use
     * {@link #tryComponentFromFilename} instead.
     *
     * @param file the filename to parse.
     * @return the {@code Descriptor} corresponding to {@code file} if it corresponds to a valid and supported sstable
     * filename, {@code null} otherwise.
     */
    public static Descriptor tryDescriptorFromFilename(File file)
    {
        try
        {
            return Descriptor.fromFilename(file);
        }
        catch (Throwable e)
        {
            return null;
        }
    }

    /** @return An estimate of the number of keys contained in the given index file. */
    public static long estimateRowsFromIndex(RandomAccessReader ifile, Descriptor descriptor) throws IOException
    {
        // collect sizes for the first 10000 keys, or first 10 mebibytes of data
        final int SAMPLES_CAP = 10000, BYTES_CAP = (int)Math.min(10000000, ifile.length());
        int keys = 0;
        while (ifile.getFilePointer() < BYTES_CAP && keys < SAMPLES_CAP)
        {
            ByteBufferUtil.skipShortLength(ifile);
            RowIndexEntry.Serializer.skip(ifile, descriptor.version);
            keys++;
        }
        assert keys > 0 && ifile.getFilePointer() > 0 && ifile.length() > 0 : "Unexpected empty index file: " + ifile;
        long estimatedRows = ifile.length() / (ifile.getFilePointer() / keys);
        ifile.seek(0);
        return estimatedRows;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "path='" + getFilename() + '\'' +
               ')';
    }

    /**
     * Registers new custom components. Used by custom compaction strategies.
     * Adding a component for the second time is a no-op.
     * Don't remove this - this method is a part of the public API, intended for use by custom compaction strategies.
     *
     * @param newComponents collection of components to be added
     */
    public synchronized void addComponents(Collection<Component> newComponents)
    {
        Collection<Component> componentsToAdd = Collections2.filter(newComponents, Predicates.not(Predicates.in(components)));
        TOCComponent.appendTOC(descriptor, componentsToAdd);
        components.addAll(componentsToAdd);
    }

    public AbstractBounds<Token> getBounds()
    {
        return AbstractBounds.bounds(first.getToken(), true, last.getToken(), true);
    }

    public static void validateRepairedMetadata(long repairedAt, TimeUUID pendingRepair, boolean isTransient)
    {
        Preconditions.checkArgument((pendingRepair == NO_PENDING_REPAIR) || (repairedAt == UNREPAIRED_SSTABLE),
                                    "pendingRepair cannot be set on a repaired sstable");
        Preconditions.checkArgument(!isTransient || (pendingRepair != NO_PENDING_REPAIR),
                                    "isTransient can only be true for sstables pending repair");

    }
}