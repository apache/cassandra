package org.apache.cassandra.io;

import java.io.File;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BloomFilter;

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
    protected String path;
    protected IPartitioner partitioner;
    protected BloomFilter bf;
    protected List<KeyPosition> indexPositions;

    /* Every 128th index entry is loaded into memory so we know where to start looking for the actual key w/o seeking */
    public static final int INDEX_INTERVAL = 128;/* Required extension for temporary files created during compactions. */
    public static final String TEMPFILE_MARKER = "tmp";

    public SSTable(String filename, IPartitioner partitioner)
    {
        assert filename.endsWith("-Data.db");
        this.path = filename;
        this.partitioner = partitioner;
    }

    protected static String indexFilename(String dataFile)
    {
        String[] parts = dataFile.split("-");
        parts[parts.length - 1] = "Index.db";
        return StringUtils.join(parts, "-");
    }

    public String indexFilename()
    {
        return indexFilename(path);
    }

    protected static String filterFilename(String dataFile)
    {
        String[] parts = dataFile.split("-");
        parts[parts.length - 1] = "Filter.db";
        return StringUtils.join(parts, "-");
    }

    public String filterFilename()
    {
        return filterFilename(path);
    }

    public String getFilename()
    {
        return path;
    }

    public static String parseTableName(String filename)
    {
        return new File(filename).getParentFile().getName();        
    }

    /**
     * This is a simple container for the index Key and its corresponding position
     * in the data file. Binary search is performed on a list of these objects
     * to lookup keys within the SSTable data file.
     *
     * All keys are decorated.
     */
    class KeyPosition implements Comparable<KeyPosition>
    {
        public final String key; // decorated
        public final long position;

        public KeyPosition(String key, long position)
        {
            this.key = key;
            this.position = position;
        }

        public int compareTo(KeyPosition kp)
        {
            return partitioner.getDecoratedKeyComparator().compare(key, kp.key);
        }

        public String toString()
        {
            return key + ":" + position;
        }
    }
}