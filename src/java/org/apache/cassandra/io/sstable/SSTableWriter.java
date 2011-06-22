/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.io.sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.OperationType;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableWriter extends SSTable
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final BufferedRandomAccessFile dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;

    public SSTableWriter(String filename, long keyCount) throws IOException
    {
        this(filename, keyCount, DatabaseDescriptor.getCFMetaData(Descriptor.fromFilename(filename)), StorageService.getPartitioner(), ReplayPosition.NONE);
    }

    public SSTableWriter(String filename, long keyCount, CFMetaData metadata, IPartitioner partitioner, ReplayPosition replayPosition) throws IOException
    {
        super(Descriptor.fromFilename(filename),
              new HashSet<Component>(Arrays.asList(Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.STATS)),
              metadata,
              replayPosition,
              partitioner,
              SSTable.defaultRowHistogram(),
              SSTable.defaultColumnHistogram());
        iwriter = new IndexWriter(descriptor, partitioner, keyCount);
        dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        dataFile = new BufferedRandomAccessFile(new File(getFilename()), "rw", BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE, true);
    }
    
    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void reset()
    {
        try
        {
            dataFile.reset(dataMark);
            iwriter.reset();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + getFilename());
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition) throws IOException
    {
        lastWrittenKey = decoratedKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        iwriter.afterAppend(decoratedKey, dataPosition);
        dbuilder.addPotentialBoundary(dataPosition);
    }

    public long append(AbstractCompactedRow row) throws IOException
    {
        long currentPosition = beforeAppend(row.key);
        ByteBufferUtil.writeWithShortLength(row.key.key, dataFile);
        row.write(dataFile);
        estimatedRowSize.add(dataFile.getFilePointer() - currentPosition);
        estimatedColumnCount.add(row.columnCount());
        afterAppend(row.key, currentPosition);
        return currentPosition;
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        // write placeholder for the row size, since we don't know it yet
        long sizePosition = dataFile.getFilePointer();
        dataFile.writeLong(-1);
        // write out row data
        int columnCount = ColumnFamily.serializer().serializeWithIndexes(cf, dataFile);
        // seek back and write the row size (not including the size Long itself)
        long endPosition = dataFile.getFilePointer();
        dataFile.seek(sizePosition);
        long dataSize = endPosition - (sizePosition + 8);
        assert dataSize > 0;
        dataFile.writeLong(dataSize);
        // finally, reset for next row
        dataFile.seek(endPosition);
        afterAppend(decoratedKey, startPosition);
        estimatedRowSize.add(endPosition - startPosition);
        estimatedColumnCount.add(columnCount);
    }

    public void append(DecoratedKey decoratedKey, ByteBuffer value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        ByteBufferUtil.writeWithShortLength(decoratedKey.key, dataFile);
        assert value.remaining() > 0;
        dataFile.writeLong(value.remaining());
        ByteBufferUtil.write(value, dataFile);
        afterAppend(decoratedKey, currentPosition);
    }

    public SSTableReader closeAndOpenReader() throws IOException
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge) throws IOException
    {
        // index and filter
        iwriter.close();

        // main data
        long position = dataFile.getFilePointer();
        dataFile.close(); // calls force
        FileUtils.truncate(dataFile.getPath(), position);

        // write sstable statistics
        writeMetadata(descriptor, estimatedRowSize, estimatedColumnCount, replayPosition);

        // remove the 'tmp' marker from all components
        final Descriptor newdesc = rename(descriptor, components);

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc, components, metadata, replayPosition, partitioner, ifile, dfile, iwriter.summary, iwriter.bf, maxDataAge, estimatedRowSize, estimatedColumnCount);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    private static void writeMetadata(Descriptor desc, EstimatedHistogram rowSizes, EstimatedHistogram columnCounts, ReplayPosition rp) throws IOException
    {
        BufferedRandomAccessFile out = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_STATS)),
                                                                     "rw",
                                                                     BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE,
                                                                     true);
        EstimatedHistogram.serializer.serialize(rowSizes, out);
        EstimatedHistogram.serializer.serialize(columnCounts, out);
        ReplayPosition.serializer.serialize(rp, out);
        out.close();
    }

    static Descriptor rename(Descriptor tmpdesc, Set<Component> components)
    {
        Descriptor newdesc = tmpdesc.asTemporary(false);
        try
        {
            // do -Data last because -Data present should mean the sstable was completely renamed before crash
            for (Component component : Sets.difference(components, Collections.singleton(Component.DATA)))
                FBUtilities.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
            FBUtilities.renameWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        return newdesc;
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }
    
    public static Builder createBuilder(Descriptor desc, OperationType type)
    {
        if (!desc.isLatestVersion)
            // TODO: streaming between different versions will fail: need support for
            // recovering other versions to provide a stable streaming api
            throw new RuntimeException(String.format("Cannot recover SSTable %s due to version mismatch. (current version is %s).", desc.toString()
                                                     , Descriptor.CURRENT_VERSION));

        return new Builder(desc, type);
    }

    /**
     * Removes the given SSTable from temporary status and opens it, rebuilding the
     * bloom filter and row index from the data file.
     */
    public static class Builder implements CompactionInfo.Holder
    {
        private final Descriptor desc;
        private final OperationType type;
        private final ColumnFamilyStore cfs;
        private RowIndexer indexer;

        public Builder(Descriptor desc, OperationType type)
        {
            this.desc = desc;
            this.type = type;
            cfs = Table.open(desc.ksname).getColumnFamilyStore(desc.cfname);
        }

        public CompactionInfo getCompactionInfo()
        {
            maybeOpenIndexer();
            try
            {
                // both file offsets are still valid post-close
                return new CompactionInfo(desc.ksname,
                                          desc.cfname,
                                          CompactionType.SSTABLE_BUILD,
                                          indexer.dfile.getFilePointer(),
                                          indexer.dfile.length());
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        // lazy-initialize the file to avoid opening it until it's actually executing on the CompactionManager,
        // since the 8MB buffers can use up heap quickly
        private void maybeOpenIndexer()
        {
            if (indexer != null)
                return;
            try
            {
                if (cfs.metadata.getDefaultValidator().isCommutative())
                    indexer = new CommutativeRowIndexer(desc, cfs, type);
                else
                    indexer = new RowIndexer(desc, cfs, type);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }

        public SSTableReader build() throws IOException
        {
            if (cfs.isInvalid())
                return null;
            maybeOpenIndexer();

            File ifile = new File(desc.filenameFor(SSTable.COMPONENT_INDEX));
            File ffile = new File(desc.filenameFor(SSTable.COMPONENT_FILTER));
            assert !ifile.exists();
            assert !ffile.exists();

            long estimatedRows = indexer.prepareIndexing();

            // build the index and filter
            long rows = indexer.index();

            logger.debug("estimated row count was {} of real count", ((double)estimatedRows) / rows);
            return SSTableReader.open(rename(desc, SSTable.componentsFor(desc, false)));
        }
    }

    static class RowIndexer
    {
        protected final Descriptor desc;
        public final BufferedRandomAccessFile dfile;
        private final OperationType type;

        protected IndexWriter iwriter;
        protected ColumnFamilyStore cfs;

        RowIndexer(Descriptor desc, ColumnFamilyStore cfs, OperationType type) throws IOException
        {
            this(desc, new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "r", 8 * 1024 * 1024, true), cfs, type);
        }

        protected RowIndexer(Descriptor desc, BufferedRandomAccessFile dfile, ColumnFamilyStore cfs, OperationType type) throws IOException
        {
            this.desc = desc;
            this.dfile = dfile;
            this.type = type;
            this.cfs = cfs;
        }

        long prepareIndexing() throws IOException
        {
            long estimatedRows;
            try
            {
                estimatedRows = SSTable.estimateRowsFromData(desc, dfile);
                iwriter = new IndexWriter(desc, StorageService.getPartitioner(), estimatedRows);
                return estimatedRows;
            }
            catch(IOException e)
            {
                dfile.close();
                throw e;
            }
        }

        long index() throws IOException
        {
            try
            {
                return doIndexing();
            }
            finally
            {
                try
                {
                    close();
                }
                catch (IOException e)
                {
                    throw new IOError(e);
                }
            }
        }

        void close() throws IOException
        {
            dfile.close();
            iwriter.close();
        }

        /*
         * If the key is cached, we should:
         *   - For AES: run the newly received row by the cache
         *   - For other: invalidate the cache (even if very unlikely, a key could be in cache in theory if a neighbor was boostrapped and
         *     then removed quickly afterward (a key that we had lost but become responsible again could have stayed in cache). That key
         *     would be obsolete and so we must invalidate the cache).
         */
        protected void updateCache(DecoratedKey key, long dataSize, AbstractCompactedRow row) throws IOException
        {
            ColumnFamily cached = cfs.getRawCachedRow(key);
            if (cached != null)
            {
                switch (type)
                {
                    case AES:
                        if (dataSize > DatabaseDescriptor.getInMemoryCompactionLimit())
                        {
                            // We have a key in cache for a very big row, that is fishy. We don't fail here however because that would prevent the sstable
                            // from being build (and there is no real point anyway), so we just invalidate the row for correction and log a warning.
                            logger.warn("Found a cached row over the in memory compaction limit during post-streaming rebuilt; it is highly recommended to avoid huge row on column family with row cache enabled.");
                            cfs.invalidateCachedRow(key);
                        }
                        else
                        {
                            ColumnFamily cf;
                            if (row == null)
                            {
                                // If not provided, read from disk.
                                long position = dfile.getFilePointer();
                                cf = ColumnFamily.create(cfs.metadata);
                                ColumnFamily.serializer().deserializeColumns(dfile, cf, true, true);
                                dfile.seek(position);
                            }
                            else
                            {
                                assert row instanceof PrecompactedRow;
                                // we do not purge so we should not get a null here
                                cf = ((PrecompactedRow)row).getFullColumnFamily();
                            }
                            cfs.updateRowCache(key, cf);
                        }
                        break;
                    default:
                        cfs.invalidateCachedRow(key);
                        break;
                }
            }
        }

        protected long doIndexing() throws IOException
        {
            EstimatedHistogram rowSizes = SSTable.defaultRowHistogram();
            EstimatedHistogram columnCounts = SSTable.defaultColumnHistogram();
            long rows = 0;
            DecoratedKey key;
            long rowPosition = 0;
            while (rowPosition < dfile.length())
            {
                // read key
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(dfile));
                iwriter.afterAppend(key, rowPosition);

                // seek to next key
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                rowPosition = dfile.getFilePointer() + dataSize;

                IndexHelper.skipBloomFilter(dfile);
                IndexHelper.skipIndex(dfile);
                ColumnFamily.serializer().deserializeFromSSTableNoColumns(ColumnFamily.create(cfs.metadata), dfile);

                // don't move that statement around, it expects the dfile to be before the columns
                updateCache(key, dataSize, null);

                rowSizes.add(dataSize);
                columnCounts.add(dfile.readInt());
                
                dfile.seek(rowPosition);

                rows++;
            }
            writeMetadata(desc, rowSizes, columnCounts, ReplayPosition.NONE);
            return rows;
        }
    }

    /*
     * When a sstable for a counter column family is streamed, we must ensure
     * that on the receiving node all counter column goes through the
     * deserialization from remote code path (i.e, it must be cleared from its
     * delta) to maintain the invariant that on a given node, only increments
     * that the node originated are delta (and copy of those must not be delta).
     *
     * Since after streaming row indexation goes through every streamed
     * sstable, we use this opportunity to ensure this property. This is the
     * goal of this specific CommutativeRowIndexer.
     */
    static class CommutativeRowIndexer extends RowIndexer
    {
        protected BufferedRandomAccessFile writerDfile;

        CommutativeRowIndexer(Descriptor desc, ColumnFamilyStore cfs, OperationType type) throws IOException
        {
            super(desc, new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "r", 8 * 1024 * 1024, true), cfs, type);
            writerDfile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_DATA)), "rw", 8 * 1024 * 1024, true);
        }

        @Override
        protected long doIndexing() throws IOException
        {
            EstimatedHistogram rowSizes = SSTable.defaultRowHistogram();
            EstimatedHistogram columnCounts = SSTable.defaultColumnHistogram();
            long rows = 0L;
            DecoratedKey key;

            CompactionController controller = new CompactionController(cfs, Collections.<SSTableReader>emptyList(), Integer.MAX_VALUE, true);
            while (!dfile.isEOF())
            {
                // read key
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, ByteBufferUtil.readWithShortLength(dfile));

                // skip data size, bloom filter, column index
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                SSTableIdentityIterator iter = new SSTableIdentityIterator(cfs.metadata, dfile, key, dfile.getFilePointer(), dataSize, true);

                AbstractCompactedRow row = controller.getCompactedRow(iter);
                updateCache(key, dataSize, row);

                rowSizes.add(dataSize);
                columnCounts.add(row.columnCount());

                // update index writer
                iwriter.afterAppend(key, writerDfile.getFilePointer());
                // write key and row
                ByteBufferUtil.writeWithShortLength(key.key, writerDfile);
                row.write(writerDfile);

                rows++;
            }
            writeMetadata(desc, rowSizes, columnCounts, ReplayPosition.NONE);

            if (writerDfile.getFilePointer() != dfile.getFilePointer())
            {
                // truncate file to new, reduced length
                writerDfile.setLength(writerDfile.getFilePointer());
            }
            writerDfile.sync();

            return rows;
        }

        @Override
        void close() throws IOException
        {
            super.close();
            writerDfile.close();
        }
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    static class IndexWriter
    {
        private final BufferedRandomAccessFile indexFile;
        public final Descriptor desc;
        public final IPartitioner partitioner;
        public final SegmentedFile.Builder builder;
        public final IndexSummary summary;
        public final BloomFilter bf;
        private FileMark mark;

        IndexWriter(Descriptor desc, IPartitioner part, long keyCount) throws IOException
        {
            this.desc = desc;
            this.partitioner = part;
            indexFile = new BufferedRandomAccessFile(new File(desc.filenameFor(SSTable.COMPONENT_INDEX)), "rw", 8 * 1024 * 1024, true);
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummary(keyCount);
            bf = BloomFilter.getFilter(keyCount, 15);
        }

        public void afterAppend(DecoratedKey key, long dataPosition) throws IOException
        {
            bf.add(key.key);
            long indexPosition = indexFile.getFilePointer();
            ByteBufferUtil.writeWithShortLength(key.key, indexFile);
            indexFile.writeLong(dataPosition);
            if (logger.isTraceEnabled())
                logger.trace("wrote index of " + key + " at " + indexPosition);

            summary.maybeAddEntry(key, indexPosition);
            builder.addPotentialBoundary(indexPosition);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        public void close() throws IOException
        {
            // bloom filter
            FileOutputStream fos = new FileOutputStream(desc.filenameFor(SSTable.COMPONENT_FILTER));
            DataOutputStream stream = new DataOutputStream(fos);
            BloomFilter.serializer().serialize(bf, stream);
            stream.flush();
            fos.getFD().sync();
            stream.close();

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

        public void reset() throws IOException
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            indexFile.reset(mark);
        }
    }
}
