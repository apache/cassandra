package org.apache.cassandra.io;

import java.io.IOException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.util.Comparator;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.config.DatabaseDescriptor;
import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    private long keysWritten;
    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;
    private String lastWrittenKey;
    private BloomFilter bf;

    public SSTableWriter(String filename, int keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));
        bf = new BloomFilter(keyCount, 15);
    }

    private long beforeAppend(String decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        Comparator<String> c = partitioner.getDecoratedKeyComparator();
        if (lastWrittenKey != null && c.compare(lastWrittenKey, decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + path);
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(String decoratedKey, long position) throws IOException
    {
        bf.add(decoratedKey);
        lastWrittenKey = decoratedKey;
        long indexPosition = indexFile.getFilePointer();
        indexFile.writeUTF(decoratedKey);
        indexFile.writeLong(position);
        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + position);

        if (keysWritten++ % INDEX_INTERVAL != 0)
            return;
        if (indexPositions == null)
        {
            indexPositions = new ArrayList<KeyPosition>();
        }
        indexPositions.add(new KeyPosition(decoratedKey, indexPosition));
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + decoratedKey + " at " + indexPosition);
    }

    // TODO make this take a DataOutputStream and wrap the byte[] version to combine them
    public void append(String decoratedKey, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(decoratedKey);
        int length = buffer.getLength();
        dataFile.writeInt(length);
        dataFile.write(buffer.getData(), 0, length);
        afterAppend(decoratedKey, currentPosition);
    }

    public void append(String decoratedKey, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(decoratedKey);
        dataFile.writeInt(value.length);
        dataFile.write(value);
        afterAppend(decoratedKey, currentPosition);
    }

    private static String rename(String tmpFilename)
    {
        String filename = tmpFilename.replace("-" + TEMPFILE_MARKER, "");
        new File(tmpFilename).renameTo(new File(filename));
        return filename;
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader(double cacheFraction) throws IOException
    {
        // bloom filter
        FileOutputStream fos = new FileOutputStream(filterFilename());
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();

        // index
        indexFile.getChannel().force(true);
        indexFile.close();

        // main data
        dataFile.close(); // calls force

        rename(indexFilename());
        rename(filterFilename());
        path = rename(path); // important to do this last since index & filter file names are derived from it

        ConcurrentLinkedHashMap<String,Long> keyCache = cacheFraction > 0
                                                        ? SSTableReader.createKeyCache((int) (cacheFraction * keysWritten))
                                                        : null;
        return new SSTableReader(path, partitioner, indexPositions, bf, keyCache);
    }

}
