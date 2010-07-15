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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.AbstractCompactedRow;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableWriter extends SSTable
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final BufferedRandomAccessFile dataFile;
    private DecoratedKey lastWrittenKey;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        iwriter = new IndexWriter(desc, partitioner, keyCount);
        dbuilder = SegmentedFile.getBuilder();
        dataFile = new BufferedRandomAccessFile(getFilename(), "rw", DatabaseDescriptor.getInMemoryCompactionLimit());
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
        dbuilder.addPotentialBoundary(dataPosition);
        iwriter.afterAppend(decoratedKey, dataPosition);
    }

    public void append(AbstractCompactedRow row) throws IOException
    {
        long currentPosition = beforeAppend(row.key);
        FBUtilities.writeShortByteArray(row.key.key, dataFile);
        row.write(dataFile);
        afterAppend(row.key, currentPosition);
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf) throws IOException
    {
        long startPosition = beforeAppend(decoratedKey);
        FBUtilities.writeShortByteArray(decoratedKey.key, dataFile);
        // write placeholder for the row size, since we don't know it yet
        long sizePosition = dataFile.getFilePointer();
        dataFile.writeLong(-1);
        // write out row data
        ColumnFamily.serializer().serializeWithIndexes(cf, dataFile);
        // seek back and write the row size (not including the size Long itself)
        long endPosition = dataFile.getFilePointer();
        dataFile.seek(sizePosition);
        dataFile.writeLong(endPosition - (sizePosition + 8));
        // finally, reset for next row
        dataFile.seek(endPosition);
        afterAppend(decoratedKey, startPosition);
    }

    public void append(DecoratedKey decoratedKey, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        FBUtilities.writeShortByteArray(decoratedKey.key, dataFile);
        assert value.length > 0;
        dataFile.writeLong(value.length);
        dataFile.write(value);
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
        dataFile.close(); // calls force

        // remove the 'tmp' marker from all components
        Descriptor newdesc = rename(desc);

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc, partitioner, ifile, dfile, iwriter.summary, iwriter.bf, maxDataAge);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    static Descriptor rename(Descriptor tmpdesc)
    {
        Descriptor newdesc = tmpdesc.asTemporary(false);
        try
        {
            for (String component : components)
                FBUtilities.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
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
    
    /**
     * @return An estimate of the number of keys contained in the given data file.
     */
    private static long estimateRows(Descriptor desc, BufferedRandomAccessFile dfile) throws IOException
    {
        // collect sizes for the first 1000 keys, or first 100 megabytes of data
        final int SAMPLES_CAP = 1000, BYTES_CAP = (int)Math.min(100000000, dfile.length());
        int keys = 0;
        long dataPosition = 0;
        while (dataPosition < BYTES_CAP && keys < SAMPLES_CAP)
        {
            dfile.seek(dataPosition);
            FBUtilities.readShortByteArray(dfile);
            long dataSize = SSTableReader.readRowSize(dfile, desc);
            dataPosition = dfile.getFilePointer() + dataSize;
            keys++;
        }
        dfile.seek(0);
        return dfile.length() / (dataPosition / keys);
    }

    /**
     * If either of the index or filter files are missing, rebuilds both.
     * TODO: Builds most of the in-memory state of the sstable, but doesn't actually open it.
     */
    private static void maybeRecover(Descriptor desc) throws IOException
    {
        File ifile = new File(desc.filenameFor(SSTable.COMPONENT_INDEX));
        File ffile = new File(desc.filenameFor(SSTable.COMPONENT_FILTER));
        if (ifile.exists() && ffile.exists())
            // nothing to do
            return;

        // remove existing files
        ifile.delete();
        ffile.delete();

        // open the data file for input, and an IndexWriter for output
        BufferedRandomAccessFile dfile = new BufferedRandomAccessFile(desc.filenameFor(SSTable.COMPONENT_DATA), "r", 8 * 1024 * 1024);
        IndexWriter iwriter;
        long estimatedRows;
        try
        {
            estimatedRows = estimateRows(desc, dfile);
            iwriter = new IndexWriter(desc, StorageService.getPartitioner(), estimatedRows);
        }
        catch(IOException e)
        {
            dfile.close();
            throw e;
        }

        // build the index and filter
        long rows = 0;
        try
        {
            DecoratedKey key;
            long dataPosition = 0;
            while (dataPosition < dfile.length())
            {
                key = SSTableReader.decodeKey(StorageService.getPartitioner(), desc, FBUtilities.readShortByteArray(dfile));
                long dataSize = SSTableReader.readRowSize(dfile, desc);
                iwriter.afterAppend(key, dataPosition);
                dataPosition = dfile.getFilePointer() + dataSize;
                dfile.seek(dataPosition);
                rows++;
            }
        }
        finally
        {
            try
            {
                dfile.close();
                iwriter.close();
            }
            catch (IOException e)
            {
                logger.error("Failed to close data or index file during recovery of " + desc, e);
            }
        }

        logger.debug("estimated row count was %s of real count", ((double)estimatedRows) / rows);
    }

    /**
     * Removes the given SSTable from temporary status and opens it, rebuilding the non-essential portions of the
     * file if necessary.
     */
    public static SSTableReader recoverAndOpen(Descriptor desc) throws IOException
    {
        if (!desc.isLatestVersion)
            // TODO: streaming between different versions will fail: need support for
            // recovering other versions to provide a stable streaming api
            throw new RuntimeException(String.format("Cannot recover SSTable with version %s (current version %s).",
                                                     desc.version, Descriptor.CURRENT_VERSION));

        maybeRecover(desc);
        return SSTableReader.open(rename(desc));
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
        
        IndexWriter(Descriptor desc, IPartitioner part, long keyCount) throws IOException
        {
            this.desc = desc;
            this.partitioner = part;
            indexFile = new BufferedRandomAccessFile(desc.filenameFor(SSTable.COMPONENT_INDEX), "rw", 8 * 1024 * 1024);
            builder = SegmentedFile.getBuilder();
            summary = new IndexSummary();
            bf = BloomFilter.getFilter(keyCount, 15);
        }

        public void afterAppend(DecoratedKey key, long dataPosition) throws IOException
        {
            bf.add(key.key);
            long indexPosition = indexFile.getFilePointer();
            FBUtilities.writeShortByteArray(key.key, indexFile);
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
            indexFile.getChannel().force(true);
            indexFile.close();

            // finalize in-memory index state
            summary.complete();
        }
    }
}
