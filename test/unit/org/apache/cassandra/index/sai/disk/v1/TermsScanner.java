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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.index.sai.disk.io.IndexFileUtils;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.ScanningPostingsReader;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsIterator;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.IndexEntry;
import org.apache.cassandra.index.sai.utils.TermsIterator;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;

public class TermsScanner implements TermsIterator
{
    private final FileHandle postingsFile;
    private final TrieTermsIterator iterator;
    private final ByteBuffer minTerm, maxTerm;
    private Pair<ByteComparable, Long> entry;

    public TermsScanner(FileHandle termFile, FileHandle postingsFile, long trieRoot)
    {
        this.postingsFile = postingsFile;
        this.iterator = new TrieTermsIterator(termFile.instantiateRebufferer(null), trieRoot);
        this.minTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(ByteSourceInverse.unescape(ByteSource.peekable(iterator.getMinTerm().asComparableBytes(ByteComparable.Version.OSS50)))));
        this.maxTerm = ByteBuffer.wrap(ByteSourceInverse.readBytes(ByteSourceInverse.unescape(ByteSource.peekable(iterator.getMaxTerm().asComparableBytes(ByteComparable.Version.OSS50)))));
    }


    @Override
    public void close()
    {
        FileUtils.closeQuietly(postingsFile);
        iterator.close();
    }

    @Override
    public ByteBuffer getMinTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer getMaxTerm()
    {
        return maxTerm;
    }

    @Override
    public IndexEntry next()
    {
        if (iterator.hasNext())
        {
            entry = iterator.next();
            return IndexEntry.create(entry.left, postings());
        }
        return null;
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    private PostingList postings()
    {
        assert entry != null;
        final IndexInput input = IndexFileUtils.instance.openInput(postingsFile);
        try
        {
            return new ScanningPostingsReader(input, new PostingsReader.BlocksSummary(input, entry.right));
        }
        catch (IOException e)
        {
            throw Throwables.unchecked(e);
        }
    }
}
