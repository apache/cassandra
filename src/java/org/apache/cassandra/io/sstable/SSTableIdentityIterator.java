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
package org.apache.cassandra.io.sstable;

import java.io.*;
import java.util.Iterator;

import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.OnDiskAtomIterator;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.serializers.MarshalException;

    public class SSTableIdentityIterator implements Comparable<SSTableIdentityIterator>, OnDiskAtomIterator
{
    private final DecoratedKey key;
    private final DataInput in;
    public final ColumnSerializer.Flag flag;

    private final ColumnFamily columnFamily;
    private final Iterator<OnDiskAtom> atomIterator;
    private final boolean validateColumns;
    private final String filename;

    // Not every SSTableIdentifyIterator is attached to a sstable, so this can be null.
    private final SSTableReader sstable;

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key)
    {
        this(sstable, file, key, false);
    }

    /**
     * Used to iterate through the columns of a row.
     * @param sstable SSTable we are reading ffrom.
     * @param file Reading using this file.
     * @param key Key of this row.
     * @param checkData if true, do its best to deserialize and check the coherence of row data
     */
    public SSTableIdentityIterator(SSTableReader sstable, RandomAccessReader file, DecoratedKey key, boolean checkData)
    {
        this(sstable.metadata, file, file.getPath(), key, checkData, sstable, ColumnSerializer.Flag.LOCAL);
    }

    /**
     * Used only by scrubber to solve problems with data written after the END_OF_ROW marker. Iterates atoms for the given dataSize only and does not accept an END_OF_ROW marker.
     */
    public static SSTableIdentityIterator createFragmentIterator(SSTableReader sstable, final RandomAccessReader file, DecoratedKey key, long dataSize, boolean checkData)
    {
        final ColumnSerializer.Flag flag = ColumnSerializer.Flag.LOCAL;
        final CellNameType type = sstable.metadata.comparator;
        final int expireBefore = (int) (System.currentTimeMillis() / 1000);
        final Version version = sstable.descriptor.version;
        final long dataEnd = file.getFilePointer() + dataSize;
        return new SSTableIdentityIterator(sstable.metadata, file, file.getPath(), key, checkData, sstable, flag, DeletionTime.LIVE,
                                           new AbstractIterator<OnDiskAtom>()
                                                   {
                                                       protected OnDiskAtom computeNext()
                                                       {
                                                           if (file.getFilePointer() >= dataEnd)
                                                               return endOfData();
                                                           try
                                                           {
                                                               return type.onDiskAtomSerializer().deserializeFromSSTable(file, flag, expireBefore, version);
                                                           }
                                                           catch (IOException e)
                                                           {
                                                               throw new IOError(e);
                                                           }
                                                       }
                                                   });
    }

    // sstable may be null *if* checkData is false
    // If it is null, we assume the data is in the current file format
    private SSTableIdentityIterator(CFMetaData metadata,
                                    FileDataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    ColumnSerializer.Flag flag)
    {
        this(metadata, in, filename, key, checkData, sstable, flag, readDeletionTime(in, sstable, filename),
             metadata.getOnDiskIterator(in, flag, (int) (System.currentTimeMillis() / 1000),
                                        sstable == null ? DatabaseDescriptor.getSSTableFormat().info.getLatestVersion() : sstable.descriptor.version));
    }

    private static DeletionTime readDeletionTime(DataInput in, SSTableReader sstable, String filename)
    {
        try
        {
            return DeletionTime.serializer.deserialize(in);
        }
        catch (IOException e)
        {
            if (sstable != null)
                sstable.markSuspect();
            throw new CorruptSSTableException(e, filename);
        }
    }

    // sstable may be null *if* checkData is false
    // If it is null, we assume the data is in the current file format
    private SSTableIdentityIterator(CFMetaData metadata,
                                    DataInput in,
                                    String filename,
                                    DecoratedKey key,
                                    boolean checkData,
                                    SSTableReader sstable,
                                    ColumnSerializer.Flag flag,
                                    DeletionTime deletion,
                                    Iterator<OnDiskAtom> atomIterator)
    {
        assert !checkData || (sstable != null);
        this.in = in;
        this.filename = filename;
        this.key = key;
        this.flag = flag;
        this.validateColumns = checkData;
        this.sstable = sstable;
        columnFamily = ArrayBackedSortedColumns.factory.create(metadata);
        columnFamily.delete(deletion);
        this.atomIterator = atomIterator;
    }

    public DecoratedKey getKey()
    {
        return key;
    }

    public ColumnFamily getColumnFamily()
    {
        return columnFamily;
    }

    public boolean hasNext()
    {
        try
        {
            return atomIterator.hasNext();
        }
        catch (IOError e)
        {
            // catch here b/c atomIterator is an AbstractIterator; hasNext reads the value
            if (e.getCause() instanceof IOException)
            {
                if (sstable != null)
                    sstable.markSuspect();
                throw new CorruptSSTableException((IOException)e.getCause(), filename);
            }
            else
            {
                throw e;
            }
        }
    }

    public OnDiskAtom next()
    {
        try
        {
            OnDiskAtom atom = atomIterator.next();
            if (validateColumns)
                atom.validateFields(columnFamily.metadata());
            return atom;
        }
        catch (MarshalException me)
        {
            throw new CorruptSSTableException(me, filename);
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        // creator is responsible for closing file when finished
    }

    public String getPath()
    {
        // if input is from file, then return that path, otherwise it's from streaming
        if (in instanceof RandomAccessReader)
        {
            RandomAccessReader file = (RandomAccessReader) in;
            return file.getPath();
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }

    public int compareTo(SSTableIdentityIterator o)
    {
        return key.compareTo(o.key);
    }
}
