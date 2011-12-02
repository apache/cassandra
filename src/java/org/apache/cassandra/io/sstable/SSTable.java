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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.DecoratedKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.cassandra.utils.Pair;

/**
 * This class is built on top of the SequenceFile. It stores
 * data on disk in sorted fashion. However the sorting is upto
 * the application. This class expects keys to be handed to it
 * in sorted order.
 *
 * A separate index file is maintained as well, containing the
 * SSTable keys and the offset into the SSTable at which they are found.
 * Every 1/indexInterval key is read into memory when the SSTable is opened.
 *
 * Finally, a bloom filter file is also kept for the keys in each SSTable.
 */
public abstract class SSTable
{
    static final Logger logger = LoggerFactory.getLogger(SSTable.class);

    // TODO: replace with 'Component' objects
    public static final String COMPONENT_DATA = Component.Type.DATA.repr;
    public static final String COMPONENT_INDEX = Component.Type.PRIMARY_INDEX.repr;
    public static final String COMPONENT_FILTER = Component.Type.FILTER.repr;
    public static final String COMPONENT_STATS = Component.Type.STATS.repr;
    public static final String COMPONENT_DIGEST = Component.Type.DIGEST.repr;

    public static final String TEMPFILE_MARKER = "tmp";

    public static final Comparator<SSTableReader> maxTimestampComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            long ts1 = o1.getMaxTimestamp();
            long ts2 = o2.getMaxTimestamp();
            return (ts1 > ts2 ? -1 : (ts1 == ts2 ? 0 : 1));
        }
    };

    public final Descriptor descriptor;
    protected final Set<Component> components;
    public final CFMetaData metadata;
    public final IPartitioner partitioner;
    public final boolean compression;

    public DecoratedKey first;
    public DecoratedKey last;

    protected SSTable(Descriptor descriptor, CFMetaData metadata, IPartitioner partitioner)
    {
        this(descriptor, new HashSet<Component>(), metadata, partitioner);
    }

    protected SSTable(Descriptor descriptor, Set<Component> components, CFMetaData metadata, IPartitioner partitioner)
    {
        // In almost all cases, metadata shouldn't be null, but allowing null allows to create a mostly functional SSTable without
        // full schema definition. SSTableLoader use that ability
        assert descriptor != null;
        assert components != null;
        assert partitioner != null;

        this.descriptor = descriptor;
        Set<Component> dataComponents = new HashSet<Component>(components);
        for (Component component : components)
            assert component.type != Component.Type.COMPACTED_MARKER;

        this.compression = dataComponents.contains(Component.COMPRESSION_INFO);
        this.components = Collections.unmodifiableSet(dataComponents);
        this.metadata = metadata;
        this.partitioner = partitioner;
    }

    public static final Comparator<SSTableReader> sstableComparator = new Comparator<SSTableReader>()
    {
        public int compare(SSTableReader o1, SSTableReader o2)
        {
            return o1.first.compareTo(o2.first);
        }
    };

    public static final Ordering<SSTableReader> sstableOrdering = Ordering.from(sstableComparator);

    /**
     * We use a ReferenceQueue to manage deleting files that have been compacted
     * and for which no more SSTable references exist.  But this is not guaranteed
     * to run for each such file because of the semantics of the JVM gc.  So,
     * we write a marker to `compactedFilename` when a file is compacted;
     * if such a marker exists on startup, the file should be removed.
     *
     * This method will also remove SSTables that are marked as temporary.
     *
     * @return true if the file was deleted
     */
    public static boolean delete(Descriptor desc, Set<Component> components) throws IOException
    {
        // remove the DATA component first if it exists
        if (components.contains(Component.DATA))
            FileUtils.deleteWithConfirm(desc.filenameFor(Component.DATA));
        for (Component component : components)
        {
            if (component.equals(Component.DATA) || component.equals(Component.COMPACTED_MARKER))
                continue;

            FileUtils.deleteWithConfirm(desc.filenameFor(component));
        }
        // remove the COMPACTED_MARKER component last if it exists
        FileUtils.delete(desc.filenameFor(Component.COMPACTED_MARKER));

        logger.debug("Deleted {}", desc);
        return true;
    }

    /**
     * If the given @param key occupies only part of a larger buffer, allocate a new buffer that is only
     * as large as necessary.
     */
    public static DecoratedKey<?> getMinimalKey(DecoratedKey<?> key)
    {
        return key.key.position() > 0 || key.key.hasRemaining()
                                       ? new DecoratedKey(key.token, HeapAllocator.instance.clone(key.key))
                                       : key;
    }

    public String getFilename()
    {
        return descriptor.filenameFor(COMPONENT_DATA);
    }

    public String getColumnFamilyName()
    {
        return descriptor.cfname;
    }

    public String getTableName()
    {
        return descriptor.ksname;
    }

    /**
     * @return A Descriptor,Component pair, or null if not a valid sstable component.
     */
    public static Pair<Descriptor,Component> tryComponentFromFilename(File dir, String name)
    {
        try
        {
            return Component.fromFilename(dir, name);
        }
        catch (Exception e)
        {
            if (!"snapshots".equals(name) && !"backups".equals(name)
                    && !name.contains(".json"))
                logger.warn("Invalid file '{}' in data directory {}.", name, dir);
            return null;
        }
    }

    /**
     * Discovers existing components for the descriptor. Slow: only intended for use outside the critical path.
     */
    static Set<Component> componentsFor(final Descriptor desc)
    {
        Set<Component> components = Sets.newHashSetWithExpectedSize(Component.TYPES.size());
        for (Component.Type componentType : Component.TYPES)
        {
            Component component = new Component(componentType);
            if (new File(desc.filenameFor(component)).exists())
                components.add(component);
        }

        return components;
    }

    /** @return An estimate of the number of keys contained in the given data file. */
    static long estimateRowsFromData(Descriptor desc, RandomAccessReader dfile) throws IOException
    {
        // collect sizes for the first 1000 keys, or first 100 megabytes of data
        final int SAMPLES_CAP = 1000, BYTES_CAP = (int)Math.min(100000000, dfile.length());
        int keys = 0;
        long dataPosition = 0;
        while (dataPosition < BYTES_CAP && keys < SAMPLES_CAP)
        {
            dfile.seek(dataPosition);
            ByteBufferUtil.skipShortLength(dfile);
            long dataSize = SSTableReader.readRowSize(dfile, desc);
            dataPosition = dfile.getFilePointer() + dataSize;
            keys++;
        }
        dfile.seek(0);
        return dfile.length() / (dataPosition / keys);
    }

    /** @return An estimate of the number of keys contained in the given index file. */
    static long estimateRowsFromIndex(RandomAccessReader ifile) throws IOException
    {
        // collect sizes for the first 10000 keys, or first 10 megabytes of data
        final int SAMPLES_CAP = 10000, BYTES_CAP = (int)Math.min(10000000, ifile.length());
        int keys = 0;
        while (ifile.getFilePointer() < BYTES_CAP && keys < SAMPLES_CAP)
        {
            ByteBufferUtil.skipShortLength(ifile);
            FileUtils.skipBytesFully(ifile, 8);
            keys++;
        }
        assert keys > 0 && ifile.getFilePointer() > 0 && ifile.length() > 0 : "Unexpected empty index file: " + ifile;
        long estimatedRows = ifile.length() / (ifile.getFilePointer() / keys);
        ifile.seek(0);
        return estimatedRows;
    }

    public static long getTotalBytes(Iterable<SSTableReader> sstables)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
        {
            sum += sstable.onDiskLength();
        }
        return sum;
    }

    public long bytesOnDisk()
    {
        long bytes = 0;
        for (Component component : components)
        {
            bytes += new File(descriptor.filenameFor(component)).length();
        }
        return bytes;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" +
               "path='" + getFilename() + '\'' +
               ')';
    }
}
