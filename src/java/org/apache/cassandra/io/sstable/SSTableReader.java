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
import java.util.*;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.Reference;
import java.nio.channels.FileChannel;
import java.nio.MappedByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.clock.AbstractReconciler;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.util.FileDataInput;

/**
 * SSTableReaders are open()ed by Table.onStart; after that they are created by SSTableWriter.renameAndOpen.
 * Do not re-call open() on existing SSTable files; use the references kept by ColumnFamilyStore post-start instead.
 */
public abstract class SSTableReader extends SSTable implements Comparable<SSTableReader>
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableReader.class);

    // `finalizers` is required to keep the PhantomReferences alive after the enclosing SSTR is itself
    // unreferenced.  otherwise they will never get enqueued.
    private static final Set<Reference<SSTableReader>> finalizers = new HashSet<Reference<SSTableReader>>();
    private static final ReferenceQueue<SSTableReader> finalizerQueue = new ReferenceQueue<SSTableReader>()
    {{
        Runnable runnable = new Runnable()
        {
            public void run()
            {
                while (true)
                {
                    SSTableDeletingReference r;
                    try
                    {
                        r = (SSTableDeletingReference) finalizerQueue.remove();
                        finalizers.remove(r);
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                    try
                    {
                        r.cleanup();
                    }
                    catch (IOException e)
                    {
                        logger.error("Error deleting " + r.path, e);
                    }
                }
            }
        };
        new Thread(runnable, "SSTABLE-DELETER").start();
    }};

    /**
     * maxDataAge is a timestamp in local server time (e.g. System.currentTimeMilli) which represents an uppper bound
     * to the newest piece of data stored in the sstable. In other words, this sstable does not contain items created
     * later than maxDataAge.
     * 
     * The field is not serialized to disk, so relying on it for more than what truncate does is not advised.
     *
     * When a new sstable is flushed, maxDataAge is set to the time of creation.
     * When a sstable is created from compaction, maxDataAge is set to max of all merged tables.
     *
     * The age is in milliseconds since epoc and is local to this host.
     */
    public final long maxDataAge;

    public static int indexInterval()
    {
        return IndexSummary.INDEX_INTERVAL;
    }

    public static long getApproximateKeyCount(Iterable<SSTableReader> sstables)
    {
        long count = 0;

        for (SSTableReader sstable : sstables)
        {
            int indexKeyCount = sstable.getKeySamples().size();
            count = count + (indexKeyCount + 1) * IndexSummary.INDEX_INTERVAL;
            if (logger.isDebugEnabled())
                logger.debug("index size for bloom filter calc for file  : " + sstable.getFilename() + "   : " + count);
        }

        return count;
    }

    public static SSTableReader open(String dataFileName) throws IOException
    {
        return open(Descriptor.fromFilename(dataFileName));
    }

    public static SSTableReader open(Descriptor desc) throws IOException
    {
        return open(desc, StorageService.getPartitioner());
    }

    /** public, but only for tests */
    public static SSTableReader open(String dataFileName, IPartitioner partitioner) throws IOException
    {
        return open(Descriptor.fromFilename(dataFileName), partitioner);
    }

    public static SSTableReader open(Descriptor descriptor, IPartitioner partitioner) throws IOException
    {
        assert partitioner != null;

        long start = System.currentTimeMillis();
        logger.info("Sampling index for " + descriptor);

        SSTableReader sstable;
        // FIXME: version conditional readers here
        if (true)
        {
            sstable = RowIndexedReader.open(descriptor, partitioner);
        }

        if (logger.isDebugEnabled())
            logger.debug("INDEX LOAD TIME for " + descriptor + ": " + (System.currentTimeMillis() - start) + " ms.");

        return sstable;
    }

    public void setTrackedBy(SSTableTracker tracker)
    {
        phantomReference = new SSTableDeletingReference(tracker, this, finalizerQueue);
        finalizers.add(phantomReference);
    }

    protected static MappedByteBuffer mmap(String filename, long start, int size) throws IOException
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile(filename, "r");
        }
        catch (FileNotFoundException e)
        {
            throw new IOError(e);
        }

        try
        {
            return raf.getChannel().map(FileChannel.MapMode.READ_ONLY, start, size);
        }
        finally
        {
            raf.close();
        }
    }

    protected SSTableReader(Descriptor desc, IPartitioner partitioner, long maxDataAge)
    {
        super(desc, partitioner);
        this.maxDataAge = maxDataAge;
    }

    private volatile SSTableDeletingReference phantomReference;

    /**
     * For testing purposes only.
     */
    public abstract void forceFilterFailures();

    /**
     * @return The key cache: for monitoring purposes.
     */
    public abstract InstrumentedCache getKeyCache();

    /**
     * @return An estimate of the number of keys in this SSTable.
     */
    public abstract long estimatedKeys();

    /**
     * @return Approximately 1/INDEX_INTERVALth of the keys in this SSTable.
     */
    public abstract Collection<DecoratedKey> getKeySamples();

    /**
     * Returns the position in the data file to find the given key, or -1 if the
     * key is not present.
     * FIXME: should not be public: use Scanner.
     */
    @Deprecated
    public abstract PositionSize getPosition(DecoratedKey decoratedKey) throws IOException;

    /**
     * Like getPosition, but if key is not found will return the location of the
     * first key _greater_ than the desired one, or -1 if no such key exists.
     * FIXME: should not be public: use Scanner.
     */
    @Deprecated
    public abstract long getNearestPosition(DecoratedKey decoratedKey) throws IOException;

    /**
     * @return The length in bytes of the data file for this SSTable.
     */
    public abstract long length();

    public void markCompacted()
    {
        if (logger.isDebugEnabled())
            logger.debug("Marking " + getFilename() + " compacted");
        try
        {
            if (!new File(compactedFilename()).createNewFile())
                throw new IOException("Unable to create compaction marker");
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        phantomReference.deleteOnCleanup();
    }

    /**
     * @param bufferSize Buffer size in bytes for this Scanner.
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract SSTableScanner getScanner(int bufferSize);

    /**
     * @param bufferSize Buffer size in bytes for this Scanner.
     * @param filter filter to use when reading the columns
     * @return A Scanner for seeking over the rows of the SSTable.
     */
    public abstract SSTableScanner getScanner(int bufferSize, QueryFilter filter);
    
    /**
     * FIXME: should not be public: use Scanner.
     */
    @Deprecated
    public abstract FileDataInput getFileDataInput(DecoratedKey decoratedKey, int bufferSize);

    public AbstractType getColumnComparator()
    {
        return DatabaseDescriptor.getComparator(getTableName(), getColumnFamilyName());
    }

    public ColumnFamily makeColumnFamily()
    {
        return ColumnFamily.create(getTableName(), getColumnFamilyName());
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
        ColumnFamilyType cfType = DatabaseDescriptor.getColumnFamilyType(getTableName(), getColumnFamilyName());
        ClockType clockType = DatabaseDescriptor.getClockType(getTableName(), getColumnFamilyName());
        AbstractReconciler reconciler = DatabaseDescriptor.getReconciler(getTableName(), getColumnFamilyName());
        return cfType == ColumnFamilyType.Standard
               ? Column.serializer(clockType)
               : SuperColumn.serializer(getColumnComparator(), clockType, reconciler);
    }

    /**
     * Tests if the sstable contains data newer than the given age param (in localhost currentMilli time).
     * This works in conjunction with maxDataAge which is an upper bound on the create of data in this sstable.
     * @param age The age to compare the maxDataAre of this sstable. Measured in millisec since epoc on this host
     * @return True iff this sstable contains data that's newer than the given age parameter.
     */
    public boolean newSince(long age)
    {
        return maxDataAge > age;
    }
}
