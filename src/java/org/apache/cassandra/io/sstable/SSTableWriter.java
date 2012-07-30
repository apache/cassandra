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

import java.io.*;
import java.nio.channels.ClosedChannelException;
import java.util.*;
import java.util.regex.Pattern;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.IColumnSerializer;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.*;

public class SSTableWriter extends SSTable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;
    private final SSTableMetadata.Collector sstableMetadataCollector;
    private final TypeSizes typeSizes = TypeSizes.NATIVE;

    public SSTableWriter(String filename, long keyCount)
    {
        this(filename,
             keyCount,
             Schema.instance.getCFMetaData(Descriptor.fromFilename(filename)),
             StorageService.getPartitioner(),
             SSTableMetadata.createCollector());
    }

    private static Set<Component> components(CFMetaData metadata)
    {
        Set<Component> components = new HashSet<Component>(Arrays.asList(Component.DATA,
                                                                         Component.FILTER,
                                                                         Component.PRIMARY_INDEX,
                                                                         Component.STATS,
                                                                         Component.SUMMARY));

        if (metadata.compressionParameters().sstableCompressor != null)
            components.add(Component.COMPRESSION_INFO);
        else
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.DIGEST);
        return components;
    }

    public SSTableWriter(String filename,
                         long keyCount,
                         CFMetaData metadata,
                         IPartitioner<?> partitioner,
                         SSTableMetadata.Collector sstableMetadataCollector)
    {
        super(Descriptor.fromFilename(filename),
              components(metadata),
              metadata,
              partitioner);
        iwriter = new IndexWriter(keyCount);

        if (compression)
        {
            dbuilder = SegmentedFile.getCompressedBuilder();
            dataFile = CompressedSequentialWriter.open(getFilename(),
                                                       descriptor.filenameFor(Component.COMPRESSION_INFO),
                                                       !DatabaseDescriptor.populateIOCacheOnFlush(),
                                                       metadata.compressionParameters(),
                                                       sstableMetadataCollector);
        }
        else
        {
            dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
            dataFile = SequentialWriter.open(new File(getFilename()), 
			                      !DatabaseDescriptor.populateIOCacheOnFlush());
            dataFile.setComputeDigest();
        }

        this.sstableMetadataCollector = sstableMetadataCollector;
    }

    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void resetAndTruncate()
    {
        dataFile.resetAndTruncate(dataMark);
        iwriter.resetAndTruncate();
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    private long beforeAppend(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null";
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getFilename());
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private RowIndexEntry afterAppend(DecoratedKey decoratedKey, long dataPosition, DeletionInfo delInfo, ColumnIndex index)
    {
        lastWrittenKey = decoratedKey;
        this.last = lastWrittenKey;
        if(null == this.first)
            this.first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        RowIndexEntry entry = RowIndexEntry.create(dataPosition, delInfo, index);
        iwriter.append(decoratedKey, entry);
        dbuilder.addPotentialBoundary(dataPosition);
        return entry;
    }

    public RowIndexEntry append(AbstractCompactedRow row)
    {
        long currentPosition = beforeAppend(row.key);
        try
        {
            ByteBufferUtil.writeWithShortLength(row.key.key, dataFile.stream);
            long dataStart = dataFile.getFilePointer();
            long dataSize = row.write(dataFile.stream);
            assert dataSize == dataFile.getFilePointer() - (dataStart + 8)
                    : "incorrect row data size " + dataSize + " written to " + dataFile.getPath() + "; correct is " + (dataFile.getFilePointer() - (dataStart + 8));
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
        sstableMetadataCollector.update(dataFile.getFilePointer() - currentPosition, row.columnStats());
        return afterAppend(row.key, currentPosition, row.deletionInfo(), row.index());
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf)
    {
        long startPosition = beforeAppend(decoratedKey);
        try
        {
            ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile.stream);

            // Since the columnIndex may insert RangeTombstone marker, computing
            // the size of the data is tricky.
            DataOutputBuffer buffer = new DataOutputBuffer();

            // build column index && write columns
            ColumnIndex.Builder builder = new ColumnIndex.Builder(cf, decoratedKey.key, cf.getColumnCount(), buffer);
            ColumnIndex index = builder.build(cf);

            TypeSizes typeSizes = TypeSizes.NATIVE;
            long delSize = DeletionTime.serializer.serializedSize(cf.deletionInfo().getTopLevelDeletion(), typeSizes);
            dataFile.stream.writeLong(buffer.getLength() + delSize + typeSizes.sizeof(0));

            // Write deletion infos + column count
            DeletionInfo.serializer().serializeForSSTable(cf.deletionInfo(), dataFile.stream);
            dataFile.stream.writeInt(builder.writtenAtomCount());
            dataFile.stream.write(buffer.getData(), 0, buffer.getLength());
            afterAppend(decoratedKey, startPosition, cf.deletionInfo(), index);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
        sstableMetadataCollector.update(dataFile.getFilePointer() - startPosition, cf.getColumnStats());
    }

    /**
     * @throws IOException if a read from the DataInput fails
     * @throws FSWriteError if a write to the dataFile fails
     */
    public long appendFromStream(DecoratedKey key, CFMetaData metadata, long dataSize, DataInput in) throws IOException
    {
        long currentPosition = beforeAppend(key);
        long dataStart;
        try
        {
            ByteBufferUtil.writeWithShortLength(key.key, dataFile.stream);
            dataStart = dataFile.getFilePointer();
            // write row size
            dataFile.stream.writeLong(dataSize);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }

        DeletionInfo deletionInfo = DeletionInfo.serializer().deserializeFromSSTable(in, descriptor.version);
        int columnCount = in.readInt();

        try
        {
            DeletionInfo.serializer().serializeForSSTable(deletionInfo, dataFile.stream);
            dataFile.stream.writeInt(columnCount);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }

        // deserialize each column to obtain maxTimestamp and immediately serialize it.
        long maxTimestamp = Long.MIN_VALUE;
        StreamingHistogram tombstones = new StreamingHistogram(TOMBSTONE_HISTOGRAM_BIN_SIZE);
        ColumnFamily cf = ColumnFamily.create(metadata, ArrayBackedSortedColumns.factory());
        cf.delete(deletionInfo);

        ColumnIndex.Builder columnIndexer = new ColumnIndex.Builder(cf, key.key, columnCount, dataFile.stream);
        OnDiskAtom.Serializer atomSerializer = cf.getOnDiskSerializer();
        for (int i = 0; i < columnCount; i++)
        {
            // deserialize column with PRESERVE_SIZE because we've written the dataSize based on the
            // data size received, so we must reserialize the exact same data
            OnDiskAtom atom = atomSerializer.deserializeFromSSTable(in, IColumnSerializer.Flag.PRESERVE_SIZE, Integer.MIN_VALUE, Descriptor.Version.CURRENT);
            if (atom instanceof CounterColumn)
            {
                atom = ((CounterColumn) atom).markDeltaToBeCleared();
            }
            else if (atom instanceof SuperColumn)
            {
                SuperColumn sc = (SuperColumn) atom;
                for (IColumn subColumn : sc.getSubColumns())
                {
                    if (subColumn instanceof CounterColumn)
                    {
                        IColumn marked = ((CounterColumn) subColumn).markDeltaToBeCleared();
                        sc.replace(subColumn, marked);
                    }
                }
            }

            int deletionTime = atom.getLocalDeletionTime();
            if (deletionTime < Integer.MAX_VALUE)
            {
                tombstones.update(deletionTime);
            }
            maxTimestamp = Math.max(maxTimestamp, atom.maxTimestamp());
            try
            {
                columnIndexer.add(atom); // This write the atom on disk too
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, dataFile.getPath());
            }
        }

        assert dataSize == dataFile.getFilePointer() - (dataStart + 8)
                : "incorrect row data size " + dataSize + " written to " + dataFile.getPath() + "; correct is " + (dataFile.getFilePointer() - (dataStart + 8));
        sstableMetadataCollector.updateMaxTimestamp(maxTimestamp);
        sstableMetadataCollector.addRowSize(dataFile.getFilePointer() - currentPosition);
        sstableMetadataCollector.addColumnCount(columnCount);
        sstableMetadataCollector.mergeTombstoneHistogram(tombstones);
        afterAppend(key, currentPosition, deletionInfo, columnIndexer.build());
        return currentPosition;
    }

    /**
     * After failure, attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public void abort()
    {
        assert descriptor.temporary;
        FileUtils.closeQuietly(iwriter);
        FileUtils.closeQuietly(dataFile);

        Set<Component> components = SSTable.componentsFor(descriptor);
        try
        {
            if (!components.isEmpty())
                SSTable.delete(descriptor, components);
        }
        catch (FSWriteError e)
        {
            logger.error(String.format("Failed deleting temp components for %s", descriptor), e);
            throw e;
        }
    }

    public SSTableReader closeAndOpenReader()
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge)
    {
        // index and filter
        iwriter.close();
        // main data, close will truncate if necessary
        dataFile.close();
        // write sstable statistics
        SSTableMetadata sstableMetadata = sstableMetadataCollector.finalizeMetadata(partitioner.getClass().getCanonicalName());
        writeMetadata(descriptor, sstableMetadata);
        maybeWriteDigest();

        // remove the 'tmp' marker from all components
        final Descriptor newdesc = rename(descriptor, components);

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc,
                                                           components,
                                                           metadata,
                                                           partitioner,
                                                           ifile,
                                                           dfile,
                                                           iwriter.summary,
                                                           iwriter.bf,
                                                           maxDataAge,
                                                           sstableMetadata);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        // try to save the summaries to disk
        SSTableReader.saveSummary(sstable, iwriter.builder, dbuilder);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    private void maybeWriteDigest()
    {
        byte[] digest = dataFile.digest();
        if (digest == null)
            return;

        SequentialWriter out = SequentialWriter.open(new File(descriptor.filenameFor(SSTable.COMPONENT_DIGEST)), true);
        // Writting output compatible with sha1sum
        Descriptor newdesc = descriptor.asTemporary(false);
        String[] tmp = newdesc.filenameFor(SSTable.COMPONENT_DATA).split(Pattern.quote(File.separator));
        String dataFileName = tmp[tmp.length - 1];
        try
        {
            out.write(String.format("%s  %s", Hex.bytesToHex(digest), dataFileName).getBytes());
        }
        catch (ClosedChannelException e)
        {
            throw new AssertionError(); // can't happen.
        }
        out.close();
    }

    private static void writeMetadata(Descriptor desc, SSTableMetadata sstableMetadata)
    {
        SequentialWriter out = SequentialWriter.open(new File(desc.filenameFor(SSTable.COMPONENT_STATS)), true);
        try
        {
            SSTableMetadata.serializer.serialize(sstableMetadata, out.stream);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getPath());
        }
        out.close();
    }

    static Descriptor rename(Descriptor tmpdesc, Set<Component> components)
    {
        Descriptor newdesc = tmpdesc.asTemporary(false);
        rename(tmpdesc, newdesc, components);
        return newdesc;
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        FileUtils.renameWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));

        // rename it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        FileUtils.renameWithOutConfirm(tmpdesc.filenameFor(Component.SUMMARY), newdesc.filenameFor(Component.SUMMARY));
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }

    public long getOnDiskFilePointer()
    {
        return dataFile.getOnDiskFilePointer();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter implements Closeable
    {
        private final SequentialWriter indexFile;
        public final SegmentedFile.Builder builder;
        public final IndexSummary summary;
        public final Filter bf;
        private FileMark mark;

        IndexWriter(long keyCount)
        {
            indexFile = SequentialWriter.open(new File(descriptor.filenameFor(SSTable.COMPONENT_INDEX)),
                                              !DatabaseDescriptor.populateIOCacheOnFlush());
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummary(keyCount);

            Double fpChance = metadata.getBloomFilterFpChance();
            if (fpChance != null && fpChance == 0)
            {
                // paranoia -- we've had bugs in the thrift <-> avro <-> CfDef dance before, let's not let that break things
                logger.error("Bloom filter FP chance of zero isn't supposed to happen");
                fpChance = null;
            }
            bf = fpChance == null ? FilterFactory.getFilter(keyCount, 15)
                                  : FilterFactory.getFilter(keyCount, fpChance);
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry)
        {
            bf.add(key.key);
            long indexPosition = indexFile.getFilePointer();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.key, indexFile.stream);
                RowIndexEntry.serializer.serialize(indexEntry, indexFile.stream);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, indexFile.getPath());
            }

            if (logger.isTraceEnabled())
                logger.trace("wrote index entry: " + indexEntry + " at " + indexPosition);

            summary.maybeAddEntry(key, indexPosition);
            builder.addPotentialBoundary(indexPosition);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        public void close()
        {
            String path = descriptor.filenameFor(SSTable.COMPONENT_FILTER);
            try
            {
                // bloom filter
                FileOutputStream fos = new FileOutputStream(path);
                DataOutputStream stream = new DataOutputStream(fos);
                FilterFactory.serialize(bf, stream, descriptor.version.filterType);
                stream.flush();
                fos.getFD().sync();
                stream.close();
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, path);
            }

            // index
            long position = indexFile.getFilePointer();
            indexFile.close(); // calls force
            FileUtils.truncate(indexFile.getPath(), position);

            // finalize in-memory index state
            summary.complete();
        }

        public void mark()
        {
            mark = indexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            indexFile.resetAndTruncate(mark);
        }

        @Override
        public String toString()
        {
            return "IndexWriter(" + descriptor + ")";
        }
    }
}
