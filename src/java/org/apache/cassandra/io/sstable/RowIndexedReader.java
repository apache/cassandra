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
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.MappedFileDataInput;

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Pre 0.7 SSTable implementation, using per row indexes.
 */
class RowIndexedReader extends SSTableReader
{
    private static final Logger logger = LoggerFactory.getLogger(RowIndexedReader.class);

    // in a perfect world, BUFFER_SIZE would be final, but we need to test with a smaller size to stay sane.
    static long BUFFER_SIZE = Integer.MAX_VALUE;

    // jvm can only map up to 2GB at a time, so we split index/data into segments of that size when using mmap i/o
    private final MappedByteBuffer[] indexBuffers;
    private final MappedByteBuffer[] buffers;

    private InstrumentedCache<Pair<Descriptor,DecoratedKey>, PositionSize> keyCache;

    RowIndexedReader(Descriptor desc,
                     IPartitioner partitioner,
                     IndexSummary indexSummary,
                     BloomFilter bloomFilter)
            throws IOException
    {
        super(desc, partitioner);

        if (DatabaseDescriptor.getIndexAccessMode() == DatabaseDescriptor.DiskAccessMode.mmap)
        {
            long indexLength = new File(indexFilename()).length();
            int bufferCount = 1 + (int) (indexLength / BUFFER_SIZE);
            indexBuffers = new MappedByteBuffer[bufferCount];
            long remaining = indexLength;
            for (int i = 0; i < bufferCount; i++)
            {
                indexBuffers[i] = mmap(indexFilename(), i * BUFFER_SIZE, (int) Math.min(remaining, BUFFER_SIZE));
                remaining -= BUFFER_SIZE;
            }
        }
        else
        {
            assert DatabaseDescriptor.getIndexAccessMode() == DatabaseDescriptor.DiskAccessMode.standard;
            indexBuffers = null;
        }

        if (DatabaseDescriptor.getDiskAccessMode() == DatabaseDescriptor.DiskAccessMode.mmap)
        {
            int bufferCount = 1 + (int) (new File(getFilename()).length() / BUFFER_SIZE);
            buffers = new MappedByteBuffer[bufferCount];
            long remaining = length();
            for (int i = 0; i < bufferCount; i++)
            {
                buffers[i] = mmap(getFilename(), i * BUFFER_SIZE, (int) Math.min(remaining, BUFFER_SIZE));
                remaining -= BUFFER_SIZE;
            }
        }
        else
        {
            assert DatabaseDescriptor.getDiskAccessMode() == DatabaseDescriptor.DiskAccessMode.standard;
            buffers = null;
        }

        this.indexSummary = indexSummary;
        this.bf = bloomFilter;
    }

    RowIndexedReader(Descriptor desc, IPartitioner partitioner) throws IOException
    {
        this(desc, partitioner, null, null);
    }

    public static RowIndexedReader open(Descriptor desc, IPartitioner partitioner) throws IOException
    {
        RowIndexedReader sstable = new RowIndexedReader(desc, partitioner);
        sstable.loadIndexFile();
        sstable.loadBloomFilter();

        return sstable;
    }

    public long estimatedKeys()
    {
        return (indexSummary.getIndexPositions().size() + 1) * IndexSummary.INDEX_INTERVAL;
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

    void loadIndexFile() throws IOException
    {
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        // any entries that do, we force into the in-memory sample so key lookup can always bsearch within
        // a single mmapped segment.
        indexSummary = new IndexSummary();
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        try
        {
            long indexSize = input.length();
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                {
                    break;
                }
                DecoratedKey decoratedKey = partitioner.convertFromDiskFormat(FBUtilities.readShortByteArray(input));
                long dataPosition = input.readLong();
                long nextIndexPosition = input.getFilePointer();
                // read the next index entry to see how big the row is
                long nextDataPosition;
                if (input.isEOF())
                {
                    nextDataPosition = length();
                }
                else
                {
                    FBUtilities.readShortByteArray(input);
                    nextDataPosition = input.readLong();
                    input.seek(nextIndexPosition);
                }
                indexSummary.maybeAddEntry(decoratedKey, dataPosition, nextDataPosition - dataPosition, indexPosition, nextIndexPosition);
            }
            indexSummary.complete();
        }
        finally
        {
            input.close();
        }
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
     * returns the position in the data file to find the given key, or -1 if the key is not present
     */
    public PositionSize getPosition(DecoratedKey decoratedKey)
    {
        // first, check bloom filter
        // FIXME: expecting utf8
        if (!bf.isPresent(new String(partitioner.convertToDiskFormat(decoratedKey), FBUtilities.UTF8)))
            return null;

        // next, the key cache
        Pair<Descriptor, DecoratedKey> unifiedKey = new Pair<Descriptor, DecoratedKey>(desc, decoratedKey);
        if (keyCache != null && keyCache.getCapacity() > 0)
        {
            PositionSize cachedPosition = keyCache.get(unifiedKey);
            if (cachedPosition != null)
            {
                return cachedPosition;
            }
        }

        // next, see if the sampled index says it's impossible for the key to be present
        IndexSummary.KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
            return null;

        // get either a buffered or a mmap'd input for the on-disk index
        long p = sampledPosition.indexPosition;
        FileDataInput input;
        try
        {
            if (indexBuffers == null)
            {
                input = new BufferedRandomAccessFile(indexFilename(), "r");
                ((BufferedRandomAccessFile)input).seek(p);
            }
            else
            {
                input = indexInputAt(p);
            }
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }

        // scan the on-disk index, starting at the nearest sampled position
        try
        {
            int i = 0;
            do
            {
                // handle exact sampled index hit
                IndexSummary.KeyPosition kp = indexSummary.getSpannedIndexPosition(input.getAbsolutePosition());
                if (kp != null && kp.key.equals(decoratedKey))
                    return indexSummary.getSpannedDataPosition(kp);

                // if using mmapped i/o, skip to the next mmap buffer if necessary
                if (input.isEOF() || kp != null)
                {
                    if (indexBuffers == null) // not mmap-ing, just one index input
                        break;

                    FileDataInput oldInput = input;
                    if (kp == null)
                    {
                        input = indexInputAt(input.getAbsolutePosition());
                    }
                    else
                    {
                        int keylength = StorageService.getPartitioner().convertToDiskFormat(kp.key).length;
                        long nextUnspannedPostion = input.getAbsolutePosition()
                                                    + DBConstants.shortSize_ + keylength
                                                    + DBConstants.longSize_;
                        input = indexInputAt(nextUnspannedPostion);
                    }
                    oldInput.close();
                    if (input == null)
                        break;

                    continue;
                }

                // read key & data position from index entry
                DecoratedKey indexDecoratedKey = partitioner.convertFromDiskFormat(FBUtilities.readShortByteArray(input));
                long dataPosition = input.readLong();

                int v = indexDecoratedKey.compareTo(decoratedKey);
                if (v == 0)
                {
                    PositionSize info = getDataPositionSize(input, dataPosition);
                    if (keyCache != null && keyCache.getCapacity() > 0)
                        keyCache.put(unifiedKey, info);
                    return info;
                }
                if (v > 0)
                    return null;
            } while  (++i < IndexSummary.INDEX_INTERVAL);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        finally
        {
            try
            {
                if (input != null)
                    input.close();
            }
            catch (IOException e)
            {
                logger.error("error closing file", e);
            }
        }
        return null;
    }

    private FileDataInput indexInputAt(long indexPosition)
    {
        if (indexPosition > indexSummary.getLastIndexPosition())
            return null;
        int bufferIndex = bufferIndex(indexPosition);
        return new MappedFileDataInput(indexBuffers[bufferIndex], indexFilename(), BUFFER_SIZE * bufferIndex, (int)(indexPosition % BUFFER_SIZE));
    }

    private PositionSize getDataPositionSize(FileDataInput input, long dataPosition) throws IOException
    {
        // if we've reached the end of the index, then the row size is "the rest of the data file"
        if (input.isEOF())
            return new PositionSize(dataPosition, length() - dataPosition);

        // otherwise, row size is the start of the next row (in next index entry), minus the start of this one.
        long nextIndexPosition = input.getAbsolutePosition();
        // if next index entry would span mmap boundary, get the next row position from the summary instead
        PositionSize nextPositionSize = indexSummary.getSpannedDataPosition(nextIndexPosition);
        if (nextPositionSize != null)
            return new PositionSize(dataPosition, nextPositionSize.position - dataPosition);

        // read next entry directly
        int utflen = input.readUnsignedShort();
        if (utflen != input.skipBytes(utflen))
            throw new EOFException();
        return new PositionSize(dataPosition, input.readLong() - dataPosition);
    }

    /** like getPosition, but if key is not found will return the location of the first key _greater_ than the desired one, or -1 if no such key exists. */
    public long getNearestPosition(DecoratedKey decoratedKey) throws IOException
    {
        IndexSummary.KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            return 0;
        }

        // can't use a MappedFileDataInput here, since we might cross a segment boundary while scanning
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        input.seek(sampledPosition.indexPosition);
        try
        {
            while (true)
            {
                DecoratedKey indexDecoratedKey;
                try
                {
                    indexDecoratedKey = partitioner.convertFromDiskFormat(FBUtilities.readShortByteArray(input));
                }
                catch (EOFException e)
                {
                    return -1;
                }
                long position = input.readLong();
                int v = indexDecoratedKey.compareTo(decoratedKey);
                if (v >= 0)
                    return position;
            }
        }
        finally
        {
            input.close();
        }
    }

    public long length()
    {
        return new File(getFilename()).length();
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
        PositionSize info = getPosition(decoratedKey);
        if (info == null)
            return null;

        if (buffers == null || (bufferIndex(info.position) != bufferIndex(info.position + info.size)))
        {
            try
            {
                BufferedRandomAccessFile file = new BufferedRandomAccessFile(getFilename(), "r", bufferSize);
                file.seek(info.position);
                return file;
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
        }
        return new MappedFileDataInput(buffers[bufferIndex(info.position)], getFilename(), BUFFER_SIZE * (info.position / BUFFER_SIZE), (int) (info.position % BUFFER_SIZE));
    }

    static int bufferIndex(long position)
    {
        return (int) (position / BUFFER_SIZE);
    }

    public InstrumentedCache getKeyCache()
    {
        return keyCache;
    }
}
