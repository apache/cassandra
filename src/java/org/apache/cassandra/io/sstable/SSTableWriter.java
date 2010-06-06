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

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOError;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.FBUtilities;

public class SSTableWriter extends SSTable
{
    private static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    private SegmentedFile.Builder ibuilder;
    private SegmentedFile.Builder dbuilder;
    private final BufferedRandomAccessFile dataFile;
    private final BufferedRandomAccessFile indexFile;
    private final BloomFilter bf;
    private DecoratedKey lastWrittenKey;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        indexSummary = new IndexSummary();
        ibuilder = SegmentedFile.getBuilder();
        dbuilder = SegmentedFile.getBuilder();
        dataFile = new BufferedRandomAccessFile(getFilename(), "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));
        bf = BloomFilter.getFilter(keyCount, 15);
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
        byte[] diskKey = partitioner.convertToDiskFormat(decoratedKey);
        bf.add(diskKey);
        lastWrittenKey = decoratedKey;
        long indexPosition = indexFile.getFilePointer();
        FBUtilities.writeShortByteArray(diskKey, indexFile);
        indexFile.writeLong(dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + dataPosition);
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + decoratedKey + " at " + indexPosition);

        indexSummary.maybeAddEntry(decoratedKey, indexPosition);
        ibuilder.addPotentialBoundary(indexPosition);
        dbuilder.addPotentialBoundary(dataPosition);
    }

    // TODO make this take a DataOutputStream and wrap the byte[] version to combine them
    public void append(DecoratedKey decoratedKey, DataOutputBuffer buffer) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        FBUtilities.writeShortByteArray(partitioner.convertToDiskFormat(decoratedKey), dataFile);
        int length = buffer.getLength();
        assert length > 0;
        dataFile.writeInt(length);
        dataFile.write(buffer.getData(), 0, length);
        afterAppend(decoratedKey, currentPosition);
    }

    public void append(DecoratedKey decoratedKey, byte[] value) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        FBUtilities.writeShortByteArray(partitioner.convertToDiskFormat(decoratedKey), dataFile);
        assert value.length > 0;
        dataFile.writeInt(value.length);
        dataFile.write(value);
        afterAppend(decoratedKey, currentPosition);
    }

    public SSTableReader closeAndOpenReader() throws IOException
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge) throws IOException
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

        // remove the 'tmp' marker from all components
        Descriptor newdesc = rename(desc);

        // finalize in-memory state for the reader
        indexSummary.complete();
        SegmentedFile ifile = ibuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(SSTable.COMPONENT_DATA));
        ibuilder = null;
        dbuilder = null;
        return RowIndexedReader.internalOpen(newdesc, partitioner, ifile, dfile, indexSummary, bf, maxDataAge);
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
    
    public static SSTableReader renameAndOpen(Descriptor tmpdesc) throws IOException
    {
        return SSTableReader.open(rename(tmpdesc));
    }
}
