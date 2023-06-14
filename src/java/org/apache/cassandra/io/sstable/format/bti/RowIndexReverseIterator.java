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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.bti.RowIndexReader.IndexInfo;
import org.apache.cassandra.io.tries.ReverseValueIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Reverse iterator over the row index. Needed to get previous index blocks for reverse iteration.
 */
class RowIndexReverseIterator extends ReverseValueIterator<RowIndexReverseIterator>
{
    private long currentNode = -1;
    private final Version version;

    public RowIndexReverseIterator(FileHandle file, long root, ByteComparable start, ByteComparable end, Version version)
    {
        super(file.instantiateRebufferer(null), root, start, end, true);
        this.version = version;
    }

    public RowIndexReverseIterator(FileHandle file, TrieIndexEntry entry, ByteComparable end, Version version)
    {
        this(file, entry.indexTrieRoot, ByteComparable.EMPTY, end, version);
    }

    public IndexInfo nextIndexInfo() throws IOException
    {
        if (currentNode == -1)
        {
            currentNode = nextPayloadedNode();
            if (currentNode == -1)
                return null;
        }

        go(currentNode);
        IndexInfo info = RowIndexReader.readPayload(buf, payloadPosition(), payloadFlags(), version);

        currentNode = -1;
        return info;
    }

    // debug/test code
    @SuppressWarnings("unused")
    public void dumpTrie(PrintStream out) throws IOException
    {
        dumpTrie(out, RowIndexReader::dumpRowIndexEntry, version);
    }
}
