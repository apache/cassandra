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

import java.io.File;
import java.io.IOException;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CloseableIterator;

public class KeyIterator extends AbstractIterator<DecoratedKey> implements CloseableIterator<DecoratedKey>
{
    private final static class In
    {
        private final File path;
        private RandomAccessReader in;

        public In(File path)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8897
            this.path = path;
        }

        private void maybeInit()
        {
            if (in == null)
                in = RandomAccessReader.open(path);
        }

        public DataInputPlus get()
        {
            maybeInit();
            return in;
        }

        public boolean isEOF()
        {
            maybeInit();
            return in.isEOF();
        }

        public void close()
        {
            if (in != null)
                in.close();
        }

        public long getFilePointer()
        {
            maybeInit();
            return in.getFilePointer();
        }

        public long length()
        {
            maybeInit();
            return in.length();
        }
    }

    private final Descriptor desc;
    private final In in;
    private final IPartitioner partitioner;

    private long keyPosition;

    public KeyIterator(Descriptor desc, TableMetadata metadata)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10232
        this.desc = desc;
        in = new In(new File(desc.filenameFor(Component.PRIMARY_INDEX)));
        partitioner = metadata.partitioner;
    }

    protected DecoratedKey computeNext()
    {
        try
        {
            if (in.isEOF())
                return endOfData();

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10661
            keyPosition = in.getFilePointer();
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8143
            DecoratedKey key = partitioner.decorateKey(ByteBufferUtil.readWithShortLength(in.get()));
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10232
            RowIndexEntry.Serializer.skip(in.get(), desc.version); // skip remainder of the entry
            return key;
        }
        catch (IOException e)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2116
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        in.close();
    }

    public long getBytesRead()
    {
        return in.getFilePointer();
    }

    public long getTotalBytes()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-2116
        return in.length();
    }

    public long getKeyPosition()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-10661
        return keyPosition;
    }
}
