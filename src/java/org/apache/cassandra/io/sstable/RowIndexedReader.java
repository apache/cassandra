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
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
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

    private static final long BUFFER_SIZE = Integer.MAX_VALUE;

    // jvm can only map up to 2GB at a time, so we split index/data into segments of that size when using mmap i/o
    private final MappedByteBuffer[] indexBuffers;
    private final MappedByteBuffer[] buffers;

    private InstrumentedCache<Pair<Descriptor,DecoratedKey>, PositionSize> keyCache;

    RowIndexedReader(Descriptor desc,
                     IPartitioner partitioner,
                     List<KeyPosition> indexPositions,
                     Map<KeyPosition, PositionSize> spannedIndexDataPositions,
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

        this.indexPositions = indexPositions;
        this.spannedIndexDataPositions = spannedIndexDataPositions;
        this.bf = bloomFilter;
    }

    RowIndexedReader(Descriptor desc, IPartitioner partitioner) throws IOException
    {
        this(desc, partitioner, null, null, null);
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
        return (indexPositions.size() + 1) * INDEX_INTERVAL;
    }

    public Collection<DecoratedKey> getKeySamples()
    {
        return Collections2.transform(indexPositions,
                                      new Function<KeyPosition,DecoratedKey>(){
                                          public DecoratedKey apply(KeyPosition kp)
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
        indexPositions = new ArrayList<KeyPosition>();
        // we read the positions in a BRAF so we don't have to worry about an entry spanning a mmap boundary.
        // any entries that do, we force into the in-memory sample so key lookup can always bsearch within
        // a single mmapped segment.
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        try
        {
            int i = 0;
            long indexSize = input.length();
            while (true)
            {
                long indexPosition = input.getFilePointer();
                if (indexPosition == indexSize)
                {
                    break;
                }
                DecoratedKey decoratedKey = partitioner.convertFromDiskFormat(input.readUTF());
                long dataPosition = input.readLong();
                long nextIndexPosition = input.getFilePointer();
                boolean spannedEntry = bufferIndex(indexPosition) != bufferIndex(nextIndexPosition);
                if (i++ % INDEX_INTERVAL == 0 || spannedEntry)
                {
                    KeyPosition info;
                    info = new KeyPosition(decoratedKey, indexPosition);
                    indexPositions.add(info);

                    if (spannedEntry)
                    {
                        if (spannedIndexDataPositions == null)
                        {
                            spannedIndexDataPositions = new HashMap<KeyPosition, PositionSize>();
                        }
                        // read the next index entry to see how big the row is corresponding to the current, mmap-segment-spanning one
                        input.readUTF();
                        long nextDataPosition = input.readLong();
                        input.seek(nextIndexPosition);
                        spannedIndexDataPositions.put(info, new PositionSize(dataPosition, nextDataPosition - dataPosition));
                    }
                }
            }
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
    private KeyPosition getIndexScanPosition(DecoratedKey decoratedKey)
    {
        assert indexPositions != null && indexPositions.size() > 0;
        int index = Collections.binarySearch(indexPositions, new KeyPosition(decoratedKey, -1));
        if (index < 0)
        {
            // binary search gives us the first index _greater_ than the key searched for,
            // i.e., its insertion position
            int greaterThan = (index + 1) * -1;
            if (greaterThan == 0)
                return null;
            return indexPositions.get(greaterThan - 1);
        }
        else
        {
            return indexPositions.get(index);
        }
    }

    /**
     * returns the position in the data file to find the given key, or -1 if the key is not present
     */
    public PositionSize getPosition(DecoratedKey decoratedKey) throws IOException
    {
        // first, check bloom filter
        if (!bf.isPresent(partitioner.convertToDiskFormat(decoratedKey)))
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
        KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            return null;
        }

        // handle exact sampled index hit
        if (spannedIndexDataPositions != null)
        {
            PositionSize info = spannedIndexDataPositions.get(sampledPosition);
            if (info != null)
                return info;
        }

        // scan the on-disk index, starting at the nearest sampled position
        long p = sampledPosition.position;
        FileDataInput input;
        if (indexBuffers == null)
        {
            input = new BufferedRandomAccessFile(indexFilename(), "r");
            ((BufferedRandomAccessFile)input).seek(p);
        }
        else
        {
            input = new MappedFileDataInput(indexBuffers[bufferIndex(p)], indexFilename(), (int)(p % BUFFER_SIZE));
        }
        try
        {
            int i = 0;
            do
            {
                DecoratedKey indexDecoratedKey;
                try
                {
                    indexDecoratedKey = partitioner.convertFromDiskFormat(input.readUTF());
                }
                catch (EOFException e)
                {
                    return null;
                }
                long position = input.readLong();
                int v = indexDecoratedKey.compareTo(decoratedKey);
                if (v == 0)
                {
                    PositionSize info;
                    if (!input.isEOF())
                    {
                        int utflen = input.readUnsignedShort();
                        if (utflen != input.skipBytes(utflen))
                            throw new EOFException();
                        info = new PositionSize(position, input.readLong() - position);
                    }
                    else
                    {
                        info = new PositionSize(position, length() - position);
                    }
                    if (keyCache != null && keyCache.getCapacity() > 0)
                        keyCache.put(unifiedKey, info);
                    return info;
                }
                if (v > 0)
                    return null;
            } while  (++i < INDEX_INTERVAL);
        }
        finally
        {
            input.close();
        }
        return null;
    }

    /** like getPosition, but if key is not found will return the location of the first key _greater_ than the desired one, or -1 if no such key exists. */
    public long getNearestPosition(DecoratedKey decoratedKey) throws IOException
    {
        KeyPosition sampledPosition = getIndexScanPosition(decoratedKey);
        if (sampledPosition == null)
        {
            return 0;
        }

        // can't use a MappedFileDataInput here, since we might cross a segment boundary while scanning
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(indexFilename(), "r");
        input.seek(sampledPosition.position);
        try
        {
            while (true)
            {
                DecoratedKey indexDecoratedKey;
                try
                {
                    indexDecoratedKey = partitioner.convertFromDiskFormat(input.readUTF());
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

    public SSTableScanner getScanner(int bufferSize) throws IOException
    {
        return new RowIndexedScanner(this, bufferSize);
    }

    public FileDataInput getFileDataInput(DecoratedKey decoratedKey, int bufferSize) throws IOException
    {
        PositionSize info = getPosition(decoratedKey);
        if (info == null)
            return null;

        if (buffers == null || (bufferIndex(info.position) != bufferIndex(info.position + info.size)))
        {
            BufferedRandomAccessFile file = new BufferedRandomAccessFile(getFilename(), "r", bufferSize);
            file.seek(info.position);
            return file;
        }
        return new MappedFileDataInput(buffers[bufferIndex(info.position)], getFilename(), (int) (info.position % BUFFER_SIZE));
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
