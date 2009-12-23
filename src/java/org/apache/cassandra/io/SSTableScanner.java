/**
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

package org.apache.cassandra.io;

import java.io.IOException;
import java.io.Closeable;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.log4j.Logger;
import com.google.common.collect.AbstractIterator;


public class SSTableScanner implements Iterator<IteratingRow>, Closeable
{
    private static Logger logger = Logger.getLogger(SSTableScanner.class);

    private IteratingRow row;
    private boolean exhausted = false;
    private BufferedRandomAccessFile file;
    private SSTableReader sstable;
    private Iterator<IteratingRow> iterator;

    /**
     * @param sstable SSTable to scan.
     * @param bufferSize Number of bytes to buffer the file while scanning.
     */
    SSTableScanner(SSTableReader sstable, int bufferSize) throws IOException
    {
        this.file = new BufferedRandomAccessFile(sstable.getFilename(), "r", bufferSize);
        this.sstable = sstable;
    }

    public void close() throws IOException
    {
        file.close();
    }

    public void seekTo(DecoratedKey seekKey)
    {
        try
        {
            long position = sstable.getNearestPosition(seekKey);
            if (position < 0)
            {
                exhausted = true;
                return;
            }
            file.seek(position);
            row = null;
        }
        catch (IOException e)
        {
            throw new RuntimeException("corrupt sstable", e);
        }
    }

    public boolean hasNext()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IteratingRow[0]).iterator() : new KeyScanningIterator();
        return iterator.hasNext();
    }

    public IteratingRow next()
    {
        if (iterator == null)
            iterator = exhausted ? Arrays.asList(new IteratingRow[0]).iterator() : new KeyScanningIterator();
        return iterator.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    private class KeyScanningIterator implements Iterator<IteratingRow>
    {
        public boolean hasNext()
        {
            try
            {
                return (row == null && !file.isEOF()) || row.getEndPosition() < file.length();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public IteratingRow next()
        {
            try
            {
                if (row != null)
                    row.skipRemaining();
                assert !file.isEOF();
                return row = new IteratingRow(file, sstable);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}
