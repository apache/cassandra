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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.InstrumentedCache;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.SegmentedFile;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Pre 0.7 SSTable implementation, using per row indexes.
 */
class RowIndexedReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(RowIndexedReader.class);

    // guesstimated size of INDEX_INTERVAL index entries
    private static final int INDEX_FILE_BUFFER_BYTES = 16 * IndexSummary.INDEX_INTERVAL;
  
    // indexfile and datafile: might be null before a call to load()
    private SegmentedFile ifile;
    private SegmentedFile dfile;
  
    private InstrumentedCache<Pair<Descriptor,DecoratedKey>, Long> keyCache;

    RowIndexedReader(Descriptor desc,
                     IPartitioner partitioner,
                     SegmentedFile ifile,
                     SegmentedFile dfile,
                     IndexSummary indexSummary,
                     BloomFilter bloomFilter,
                     long maxDataAge)
            throws IOException
    {
        super(desc, partitioner, maxDataAge);


        this.ifile = ifile;
        this.dfile = dfile;
        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
    }

    /** Open a RowIndexedReader which needs its state loaded from disk. */
    static RowIndexedReader internalOpen(Descriptor desc, IPartitioner partitioner) throws IOException
    {
        RowIndexedReader sstable = new RowIndexedReader(desc, partitioner, null, null, null, null, System.currentTimeMillis());

        // versions before 'c' encoded keys as utf-16 before hashing to the filter
        if (desc.versionCompareTo("c") < 0)
            sstable.load(true);
        else
        {
            sstable.load(false);
            sstable.loadBloomFilter();
        }

        return sstable;
    }

    /**
     * Open a RowIndexedReader which already has its state initialized (by SSTableWriter).
     */
    static RowIndexedReader internalOpen(Descriptor desc, IPartitioner partitioner, SegmentedFile ifile, SegmentedFile dfile, IndexSummary isummary, BloomFilter bf, long maxDataAge) throws IOException
    {
        assert desc != null && partitioner != null && ifile != null && dfile != null && isummary != null && bf != null;
        return new RowIndexedReader(desc, partitioner, ifile, dfile, isummary, bf, maxDataAge);
    }

    public long estimatedKeys()
    {
        return indexSummary.getIndexPositions().size() * IndexSummary.INDEX_INTERVAL;
    }

    public Collection<DecoratedKey> getKeySamples()
    {
        return Collections2.transform(indexSummary.getIndexPositions(),
                                      new Function<IndexSummary.KeyPosition, DecoratedKey>(){
                                          public DecoratedKey apply(IndexSummary.KeyPosition kp)
                                          {
                                              return kp.key;
                                          }
                                      });
    }

    void loadBloomFilter() throws IOException
    {
        DataInputStream stream = new DataInputStream(new FileInputStream(filterFilename()));
        try
        {
            bf = BloomFilter.serializer().deserialize(stream);
        }
        finally
        {
            stream.close();
        }
    }

    /**
     * Loads ifile, dfile and indexSummary, and optionally recreates the bloom filter.
     */
    private void load(boolean recreatebloom) throws IOException
    {
        SegmentedFile.Builder ibuilder = SegmentedFile.getBuilder();
        SegmentedFile.Builder dbuilder = SegmentedFile.getBuilder();

        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        indexSummary = new IndexSummary();
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        try
        {
            long indexSize = input.length();
            if (recreatebloom)
                // estimate key count based on index length
                bf = BloomFilter.getFilter((int)(input.length() / 32), 15);
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                    break;

                DecoratedKey decoratedKey = partitioner.convertFromDiskFormat(FBUtilities.readShortByteArray(input));
                if (recreatebloom)
                    bf.add(decoratedKey.key);
                long dataPosition = input.readLong();

                indexSummary.maybeAddEntry(decoratedKey, indexPosition);
                ibuilder.addPotentialBoundary(indexPosition);
                dbuilder.addPotentialBoundary(dataPosition);
            }
            indexSummary.complete();
        }
        finally
        {
            input.close();
        }

        // finalize the state of the reader
        indexSummary.complete();
        ifile = ibuilder.complete(indexFilename());
        dfile = dbuilder.complete(getFilename());
    }

    @Override
    public void setTrackedBy(SSTableTracker tracker)
    {
        super.setTrackedBy(tracker);
        keyCache = tracker.getKeyCache();
    }

    /** get the position in the index file to start scanning to find the given key (at most indexInterval keys away) */
    private IndexSummary.KeyPosition getIndexScanPosition(DecoratedKey decoratedKey)
    {
        assert indexSummary.getIndexPositions() != null && indexSummary.getIndexPositions().size() > 0;
        int index = Collections.binarySearch(indexSummary.getIndexPositions(), new IndexSummary.KeyPosition(decoratedKey, -1));
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return null;
            return indexSummary.getIndexPositions().get(greaterThan - 1);
        }
        else
        {
            return indexSummary.getIndexPositions().get(index);
        }
    }

    /**
     * @return The position in the data file to find the given key, or -1 if the key is not present
     */
    public long getPosition(DecoratedKey decoratedKey)
    {
        // first, check bloom filter
        if (!bf.isPresent(partitioner.convertToDiskFormat(decoratedKey)))
            return -1;

        // next, the key cache
        Pair<Descriptor, DecoratedKey> unifiedKey = new Pair<Descriptor, DecoratedKey>(desc, decoratedKey);
        if (keyCache != null && keyCache.getCapacity() > 0)
        {
            Long cachedPosition = keyCache.get(unifiedKey);
            if (cachedPosition != null)
            {
                return cachedPosition;
            }
        }

        // next, see if the sampled index says it's impossible for the key to be present
        IndexSummary.KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
            return -1;

        // scan the on-disk index, starting at the nearest sampled position
        int i = 0;
        Iterator<FileDataInput> segments = ifile.iterator(sampledPosition.indexPosition, INDEX_FILE_BUFFER_BYTES);
        while (segments.hasNext())
        {
            FileDataInput input = segments.next();
            try
            {
                while (!input.isEOF() && i++ < IndexSummary.INDEX_INTERVAL)
                {
                    // read key & data position from index entry
                    DecoratedKey indexDecoratedKey = partitioner.convertFromDiskFormat(FBUtilities.readShortByteArray(input));
                    long dataPosition = input.readLong();

                    int v = indexDecoratedKey.compareTo(decoratedKey);
                    if (v == 0)
                    {
                        if (keyCache != null && keyCache.getCapacity() > 0)
                            keyCache.put(unifiedKey, Long.valueOf(dataPosition));
                        return dataPosition;
                    }
                    if (v > 0)
                        return -1;
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    logger.error("error closing file", e);
                }
            }
        }
        return -1;
    }

    /**
     * @return The location of the first key _greater_ than the desired one, or -1 if no such key exists.
     */
    public long getNearestPosition(DecoratedKey decoratedKey)
    {
        IndexSummary.KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
            return 0;

        // scan the on-disk index, starting at the nearest sampled position
        Iterator<FileDataInput> segiter = ifile.iterator(sampledPosition.indexPosition, INDEX_FILE_BUFFER_BYTES);
        while (segiter.hasNext())
        {
            FileDataInput input = segiter.next();
            try
            {
                while (!input.isEOF())
                {
                    DecoratedKey indexDecoratedKey = partitioner.convertFromDiskFormat(FBUtilities.readShortByteArray(input));
                    long position = input.readLong();
                    int v = indexDecoratedKey.compareTo(decoratedKey);
                    if (v >= 0)
                        return position;
                }
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            finally
            {
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    logger.error("error closing file", e);
                }
            }
        }
        return -1;
    }

    public long length()
    {
        return dfile.length;
    }

    public int compareTo(SSTableReader o)
    {
        return desc.generation - o.desc.generation;
    }

    public void forceFilterFailures()
    {
        bf = BloomFilter.alwaysMatchingBloomFilter();
    }

    public SSTableScanner getScanner(int bufferSize)
    {
        return new RowIndexedScanner(this, bufferSize);
    }

    public SSTableScanner getScanner(int bufferSize, QueryFilter filter)
    {
        return new RowIndexedScanner(this, filter, bufferSize);
    }
    
    public FileDataInput getFileDataInput(DecoratedKey decoratedKey, int bufferSize)
    {
        long position = getPosition(decoratedKey);
        if (position < 0)
            return null;

        return dfile.getSegment(position, bufferSize);
    }

    public InstrumentedCache getKeyCache()
    {
        return keyCache;
    }
}
