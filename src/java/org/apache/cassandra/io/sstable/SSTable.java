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
import java.io.IOException;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.db.DecoratedKey;

import com.google.common.base.Objects;

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
    static final Logger logger = Logger.getLogger(SSTable.class);

    public static final int FILES_ON_DISK = 3; // data, index, and bloom filter
    public static final String COMPONENT_DATA = "Data.db";
    public static final String COMPONENT_INDEX = "Index.db";
    public static final String COMPONENT_FILTER = "Filter.db";

    public static final String COMPONENT_COMPACTED = "Compacted";

    protected Descriptor desc;
    protected IPartitioner partitioner;
    protected BloomFilter bf;
    protected List<KeyPosition> indexPositions;
    protected Map<KeyPosition, PositionSize> spannedIndexDataPositions; // map of index position, to data position, for index entries spanning mmap segments

    /* Every 128th index entry is loaded into memory so we know where to start looking for the actual key w/o seeking */
    public static final int INDEX_INTERVAL = 128;/* Required extension for temporary files created during compactions. */
    public static final String TEMPFILE_MARKER = "tmp";

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

    public IPartitioner getPartitioner()
    {
        return partitioner;
    }

    public Descriptor getDescriptor()
    {
        return desc;
    }

    protected static String parseColumnFamilyName(String filename)
    {
        return new File(filename).getName().split("-")[0];
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
    public static boolean deleteIfCompacted(String dataFilename) throws IOException
    {
        if (new File(compactedFilename(dataFilename)).exists())
        {
            FileUtils.deleteWithConfirm(new File(dataFilename));
            FileUtils.deleteWithConfirm(new File(SSTable.indexFilename(dataFilename)));
            FileUtils.deleteWithConfirm(new File(SSTable.filterFilename(dataFilename)));
            FileUtils.deleteWithConfirm(new File(SSTable.compactedFilename(dataFilename)));
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

    /** @return component names for files associated w/ this SSTable */
    public List<String> getAllComponents()
    {
        // TODO streaming relies on the -Data (getFilename) file to be last, this is clunky
        return Arrays.asList(COMPONENT_FILTER, COMPONENT_INDEX, COMPONENT_DATA);
    }

    public String getColumnFamilyName()
    {
        return desc.cfname;
    }

    public String getTableName()
    {
        return desc.ksname;
    }

    public static String parseTableName(String filename)
    {
        return Descriptor.fromFilename(filename).ksname;        
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

    /**
     * This is a simple container for the index Key and its corresponding position
     * in the data file. Binary search is performed on a list of these objects
     * to lookup keys within the SSTable data file.
     */
    public static class KeyPosition implements Comparable<KeyPosition>
    {
        public final DecoratedKey key;
        public final long position;

        public KeyPosition(DecoratedKey key, long position)
        {
            this.key = key;
            this.position = position;
        }

        public int compareTo(KeyPosition kp)
        {
            return key.compareTo(kp.key);
        }

        public String toString()
        {
            return key + ":" + position;
        }
    }

    public long bytesOnDisk()
    {
        long bytes = 0;
        for (String cname : getAllComponents())
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

    public static class PositionSize
    {
        public final long position;
        public final long size;

        public PositionSize(long position, long size)
        {
            this.position = position;
            this.size = size;
        }
    }

    /**
     * A SSTable is described by the keyspace and column family it contains data
     * for, a generation (where higher generations contain more recent data) and
     * an alphabetic version string.
     *
     * A descriptor can be marked as temporary, which influences generated filenames.
     */
    public static class Descriptor
    {
        public static final String LEGACY_VERSION = "a";
        public static final String CURRENT_VERSION = "b";

        public final File directory;
        public final String version;
        public final String ksname;
        public final String cfname;
        public final int generation;
        public final boolean temporary;
        private final int hashCode;

        /**
         * A descriptor that assumes CURRENT_VERSION.
         */
        public Descriptor(File directory, String ksname, String cfname, int generation, boolean temp)
        {
            this(CURRENT_VERSION, directory, ksname, cfname, generation, temp);
        }

        public Descriptor(String version, File directory, String ksname, String cfname, int generation, boolean temp)
        {
            assert version != null && directory != null && ksname != null && cfname != null;
            this.version = version;
            this.directory = directory;
            this.ksname = ksname;
            this.cfname = cfname;
            this.generation = generation;
            temporary = temp;
            hashCode = Objects.hashCode(directory, generation, ksname, cfname);
        }

        /**
         * @param suffix A component suffix, such as 'Data.db'/'Index.db'/etc
         * @return A filename for this descriptor with the given suffix.
         */
        public String filenameFor(String suffix)
        {
            StringBuilder buff = new StringBuilder();
            buff.append(directory).append(File.separatorChar);
            buff.append(cfname).append("-");
            if (temporary)
                buff.append(TEMPFILE_MARKER).append("-");
            if (!LEGACY_VERSION.equals(version))
                buff.append(version).append("-");
            buff.append(generation).append("-");
            buff.append(suffix);
            return buff.toString();
        }

        /**
         * Filename of the form "<ksname>/<cfname>-[tmp-][<version>-]<gen>-*"
         * @param filename A full SSTable filename, including the directory.
         * @return A SSTable.Descriptor for the filename. 
         */
        public static Descriptor fromFilename(String filename)
        {
            int separatorPos = filename.lastIndexOf(File.separatorChar);
            assert separatorPos != -1 : "Filename must include parent directory.";
            File directory = new File(filename.substring(0, separatorPos));
            String name = filename.substring(separatorPos+1, filename.length());

            // name of parent directory is keyspace name
            String ksname = directory.getName();

            // tokenize the filename
            StringTokenizer st = new StringTokenizer(name, "-");
            String nexttok = null;
            
            // all filenames must start with a column family
            String cfname = st.nextToken();

            // optional temporary marker
            nexttok = st.nextToken();
            boolean temporary = false;
            if (nexttok.equals(TEMPFILE_MARKER))
            {
                temporary = true;
                nexttok = st.nextToken();
            }

            // optional version string
            String version = LEGACY_VERSION;
            if (versionValidate(nexttok))
            {
                version = nexttok;
                nexttok = st.nextToken();
            }
            int generation = Integer.parseInt(nexttok);

            return new Descriptor(version, directory, ksname, cfname, generation, temporary);
        }
        
        /**
         * @return A clone of this descriptor with the given 'temporary' status.
         */
        public Descriptor asTemporary(boolean temporary)
        {
            return new Descriptor(version, directory, ksname, cfname, generation, temporary);
        }

        /**
         * @return True if the given version string is not empty, and
         * contains all lowercase letters, as defined by java.lang.Character.
         */
        private static boolean versionValidate(String ver)
        {
            if (ver.length() < 1) return false;
            for (char ch : ver.toCharArray())
                if (!Character.isLetter(ch) || !Character.isLowerCase(ch))
                    return false;
            return true;
        }

        @Override
        public String toString()
        {
            return this.filenameFor("<>");
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == this)
                return true;
            if (!(o instanceof Descriptor))
                return false;
            Descriptor that = (Descriptor)o;
            return that.directory.equals(this.directory) && that.generation == this.generation && that.ksname.equals(this.ksname) && that.cfname.equals(this.cfname);
        }

        @Override
        public int hashCode()
        {
            return hashCode;
        }
    }
}
