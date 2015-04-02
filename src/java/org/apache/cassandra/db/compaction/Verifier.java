/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.*;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.FileDigestValidator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Verifier implements Closeable
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;

    private final CompactionController controller;


    private final RandomAccessReader dataFile;
    private final RandomAccessReader indexFile;
    private final VerifyInfo verifyInfo;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    private int goodRows;
    private int badRows;

    private final OutputHandler outputHandler;
    private FileDigestValidator validator;

    public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, boolean isOffline) throws IOException
    {
        this(cfs, sstable, new OutputHandler.LogOutput(), isOffline);
    }

    public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, OutputHandler outputHandler, boolean isOffline) throws IOException
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.outputHandler = outputHandler;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata);

        this.controller = new VerifyController(cfs);

        this.dataFile = isOffline
                        ? sstable.openDataReader()
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter());
        this.indexFile = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)));
        this.verifyInfo = new VerifyInfo(dataFile, sstable);
    }

    public void verify(boolean extended) throws IOException
    {
        long rowStart = 0;

        outputHandler.output(String.format("Verifying %s (%s bytes)", sstable, dataFile.length()));
        outputHandler.output(String.format("Checking computed hash of %s ", sstable));


        // Verify will use the adler32 Digest files, which works for both compressed and uncompressed sstables
        try
        {
            validator = null;

            if (new File(sstable.descriptor.filenameFor(Component.DIGEST)).exists())
            {
                validator = DataIntegrityMetadata.fileDigestValidator(sstable.descriptor);
                validator.validate();
            }
            else
            {
                outputHandler.output("Data digest missing, assuming extended verification of disk atoms");
                extended = true;
            }
        }
        catch (IOException e)
        {
            outputHandler.debug(e.getMessage());
            markAndThrow();
        }
        finally
        {
            FileUtils.closeQuietly(validator);
        }

        if ( !extended )
            return;

        outputHandler.output("Extended Verify requested, proceeding to inspect atoms");


        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserialize(indexFile, sstable.descriptor.version).position;
                if (firstRowPositionFromIndex != 0)
                    markAndThrow();
            }

            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {

                if (verifyInfo.isStopRequested())
                    throw new CompactionInterruptedException(verifyInfo.getCompactionInfo());

                rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                long dataSize = -1;
                try
                {
                    key = sstable.partitioner.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                ByteBuffer currentIndexKey = nextIndexKey;
                long nextRowPositionFromIndex = 0;
                try
                {
                    nextIndexKey = indexFile.isEOF() ? null : ByteBufferUtil.readWithShortLength(indexFile);
                    nextRowPositionFromIndex = indexFile.isEOF()
                                             ? dataFile.length()
                                             : rowIndexEntrySerializer.deserialize(indexFile, sstable.descriptor.version).position;
                }
                catch (Throwable th)
                {
                    markAndThrow();
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                        ? -1
                                        : rowStart + 2 + currentIndexKey.remaining();

                dataSize = nextRowPositionFromIndex - dataStartFromIndex;
                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug(String.format("row %s is %s bytes", keyName, dataSize));

                assert currentIndexKey != null || indexFile.isEOF();

                try
                {
                    if (key == null || dataSize > dataFile.length())
                        markAndThrow();

                    //mimic the scrub read path
                    new SSTableIdentityIterator(sstable, dataFile, key, true);
                    if ( (prevKey != null && prevKey.compareTo(key) > 0) || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex )
                        markAndThrow();
                    
                    goodRows++;
                    prevKey = key;


                    outputHandler.debug(String.format("Row %s at %s valid, moving to next row at %s ", goodRows, rowStart, nextRowPositionFromIndex));
                    dataFile.seek(nextRowPositionFromIndex);
                }
                catch (Throwable th)
                {
                    badRows++;
                    markAndThrow();
                }
            }
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(t);
        }
        finally
        {
            controller.close();
        }

        outputHandler.output("Verify of " + sstable + " succeeded. All " + goodRows + " rows read successfully");
    }

    public void close()
    {
        FileUtils.closeQuietly(dataFile);
        FileUtils.closeQuietly(indexFile);
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    private void markAndThrow() throws IOException
    {
        sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE);
        throw new CorruptSSTableException(new Exception(String.format("Invalid SSTable %s, please force repair", sstable.getFilename())), sstable.getFilename());
    }

    public CompactionInfo.Holder getVerifyInfo()
    {
        return verifyInfo;
    }

    private static class VerifyInfo extends CompactionInfo.Holder
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;

        public VerifyInfo(RandomAccessReader dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.VERIFY,
                                          dataFile.getFilePointer(),
                                          dataFile.length());
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }

    private static class VerifyController extends CompactionController
    {
        public VerifyController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public long maxPurgeableTimestamp(DecoratedKey key)
        {
            return Long.MIN_VALUE;
        }
    }
}
