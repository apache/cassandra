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
import java.io.IOError;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.db.StatisticsTable;
import org.apache.cassandra.utils.EstimatedHistogram;

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

    public static final int FILES_ON_DISK = 3; // data, index, and bloom filter
    public static final String COMPONENT_DATA = "Data.db";
    public static final String COMPONENT_INDEX = "Index.db";
    public static final String COMPONENT_FILTER = "Filter.db";

    public static final String COMPONENT_COMPACTED = "Compacted";

    protected Descriptor desc;
    protected IPartitioner partitioner;

    public static final String TEMPFILE_MARKER = "tmp";

    public static List<String> components = Collections.unmodifiableList(Arrays.asList(COMPONENT_FILTER, COMPONENT_INDEX, COMPONENT_DATA));
    protected EstimatedHistogram estimatedRowSize = new EstimatedHistogram(130);
    protected EstimatedHistogram estimatedColumnCount = new EstimatedHistogram(112);

    protected SSTable(String filename, IPartitioner partitioner)
    {
        assert filename.endsWith("-" + COMPONENT_DATA);
        this.desc = Descriptor.fromFilename(filename);
        this.partitioner = partitioner;
    }

    protected SSTable(Descriptor desc, IPartitioner partitioner)
    {
        this.desc = desc;
        this.partitioner = partitioner;
    }

    public EstimatedHistogram getEstimatedRowSize()
    {
        return estimatedRowSize;
    }

    public EstimatedHistogram getEstimatedColumnCount()
    {
        return estimatedColumnCount;
    }

    public IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public Descriptor getDescriptor()
    {
        return desc;
    }

    public static String indexFilename(String dataFile)
    {
        return Descriptor.fromFilename(dataFile).filenameFor(COMPONENT_INDEX);
    }

    public String indexFilename()
    {
        return desc.filenameFor(COMPONENT_INDEX);
    }

    protected static String compactedFilename(String dataFile)
    {
        return Descriptor.fromFilename(dataFile).filenameFor(COMPONENT_COMPACTED);
    }

    /**
     * We use a ReferenceQueue to manage deleting files that have been compacted
     * and for which no more SSTable references exist.  But this is not guaranteed
     * to run for each such file because of the semantics of the JVM gc.  So,
     * we write a marker to `compactedFilename` when a file is compacted;
     * if such a marker exists on startup, the file should be removed.
     *
     * @return true if the file was deleted
     */
    public static boolean deleteIfCompacted(String dataFilename)
    {
        if (new File(compactedFilename(dataFilename)).exists())
        {
            try
            {
                FileUtils.deleteWithConfirm(new File(dataFilename));
                FileUtils.deleteWithConfirm(new File(SSTable.indexFilename(dataFilename)));
                FileUtils.deleteWithConfirm(new File(SSTable.filterFilename(dataFilename)));
                FileUtils.deleteWithConfirm(new File(SSTable.compactedFilename(dataFilename)));
                StatisticsTable.deleteSSTableStatistics(dataFilename);
            }
            catch (IOException e)
            {
                throw new IOError(e);
            }
            logger.info("Deleted " + dataFilename);
            return true;
        }
        return false;
    }

    protected String compactedFilename()
    {
        return desc.filenameFor(COMPONENT_COMPACTED);
    }

    protected static String filterFilename(String dataFile)
    {
        return Descriptor.fromFilename(dataFile).filenameFor(COMPONENT_FILTER);
    }

    public String filterFilename()
    {
        return desc.filenameFor(COMPONENT_FILTER);
    }

    public String getFilename()
    {
        return desc.filenameFor(COMPONENT_DATA);
    }

    public String getColumnFamilyName()
    {
        return desc.cfname;
    }

    public String getTableName()
    {
        return desc.ksname;
    }

    public static long getTotalBytes(Iterable<SSTableReader> sstables)
    {
        long sum = 0;
        for (SSTableReader sstable : sstables)
        {
            sum += sstable.length();
        }
        return sum;
    }

    public long bytesOnDisk()
    {
        long bytes = 0;
        for (String cname : components)
        {
            bytes += new File(desc.filenameFor(cname)).length();
        }
        return bytes;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "(" +
               "path='" + getFilename() + '\'' +
               ')';
    }
}
