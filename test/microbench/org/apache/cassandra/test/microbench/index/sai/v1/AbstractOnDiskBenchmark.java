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

package org.apache.cassandra.test.microbench.index.sai.v1;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifierFactory;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.store.IndexInput;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public abstract class AbstractOnDiskBenchmark
{
    private static Random random = new Random();

    private Descriptor descriptor;

    IndexComponents groupComponents;
    private FileHandle token;

    private IndexComponents indexComponents;
    private FileHandle postings;
    private long summaryPosition;

    /**
     * @return num of rows to be stored in per-sstable components
     */
    public abstract int numRows();

    /**
     * @return num of postings to be written in posting file
     */
    public abstract int numPostings();

    /**
     * To be called before executing each @Benchmark method
     */
    public abstract void beforeInvocation() throws Throwable;

    /**
     * To be called after executing each @Benchmark method
     */
    public abstract void afterInvocation() throws Throwable;

    protected int toPosting(int id)
    {
        return id;
    }

    protected long toToken(long id)
    {
        return id * 16_013L + random.nextInt(16_000);
    }

    protected long toOffset(long id)
    {
        return id * 16_013L + random.nextInt(16_000);
    }

    @Setup(Level.Trial)
    public void perTrialSetup() throws IOException
    {
        DatabaseDescriptor.daemonInitialization(); // required to use ChunkCache
        assert ChunkCache.instance != null;

        descriptor = new Descriptor(Files.createTempDirectory("jmh").toFile(), "ks", this.getClass().getSimpleName(), SSTableUniqueIdentifierFactory.instance.defaultBuilder().generator(Stream.empty()).get());
        groupComponents = IndexComponents.perSSTable(descriptor, null);
        indexComponents = IndexComponents.create("col", descriptor, null);

        // write per-sstable components: token and offset
        writeSSTableComponents(numRows());
        token = groupComponents.createFileHandle(IndexComponents.TOKEN_VALUES);

        // write postings
        summaryPosition = writePostings(numPostings());
        postings = indexComponents.createFileHandle(indexComponents.postingLists);
    }

    @TearDown(Level.Trial)
    public void perTrialTearDown()
    {
        token.close();
        postings.close();
        FileUtils.deleteRecursive(descriptor.directory);
    }

    @Setup(Level.Invocation)
    public void perInvocationSetup() throws Throwable
    {
        beforeInvocation();
    }

    @TearDown(Level.Invocation)
    public void perInvocationTearDown() throws Throwable
    {
        afterInvocation();
    }

    private long writePostings(int rows) throws IOException
    {
        final int[] postings = IntStream.range(0, rows).map(this::toPosting).toArray();
        final ArrayPostingList postingList = new ArrayPostingList(postings);

        try (PostingsWriter writer = new PostingsWriter(indexComponents, false))
        {
            long summaryPosition = writer.write(postingList);
            writer.complete();

            return summaryPosition;
        }
    }

    protected final PostingsReader openPostingsReader() throws IOException
    {
        IndexInput input = indexComponents.openInput(postings);
        IndexInput summaryInput = indexComponents.openInput(postings);

        PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryPosition);
        return new PostingsReader(input, summary, QueryEventListener.PostingListEventListener.NO_OP);
    }

    private void writeSSTableComponents(int rows) throws IOException
    {
        SSTableComponentsWriter writer = new SSTableComponentsWriter(descriptor, null);
        for (int i = 0; i < rows; i++)
            writer.recordCurrentTokenOffset(toToken(i), toOffset(i));

        writer.complete();
    }

    protected final LongArray openRowIdToTokenReader() throws IOException
    {
        MetadataSource source = MetadataSource.loadGroupMetadata(groupComponents);
        return new BlockPackedReader(token, IndexComponents.TOKEN_VALUES, groupComponents, source).open();
    }
}
