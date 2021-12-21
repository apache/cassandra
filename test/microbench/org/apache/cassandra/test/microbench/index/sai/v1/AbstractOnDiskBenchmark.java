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

import com.google.common.base.Stopwatch;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifierFactory;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.lucene.store.IndexInput;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public abstract class AbstractOnDiskBenchmark
{
    private static Random random = new Random();

    protected TableMetadata metadata;
    protected IndexDescriptor indexDescriptor;

    private Descriptor descriptor;

    String index;
    IndexContext indexContext;
    private FileHandle token;
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

        String keyspaceName = "ks";
        String tableName = this.getClass().getSimpleName();
        metadata = TableMetadata
                   .builder(keyspaceName, tableName)
                   .partitioner(Murmur3Partitioner.instance)
                   .addPartitionKeyColumn("pk", UTF8Type.instance)
                   .addRegularColumn("col", IntegerType.instance)
                   .build();

        descriptor = new Descriptor(new File(Files.createTempDirectory("jmh").toFile()),
                                    metadata.keyspace,
                                    metadata.name,
                                    SSTableUniqueIdentifierFactory.instance.defaultBuilder().generator(Stream.empty()).get());
        indexDescriptor = IndexDescriptor.create(descriptor, metadata.partitioner, metadata.comparator);
        index = "test";
        indexContext = SAITester.createIndexContext(index, IntegerType.instance);

        // write per-sstable components: token and offset
        writeSSTableComponents(numRows());
        token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);

        // write postings
        summaryPosition = writePostings(numPostings());
        postings = indexDescriptor.createPerIndexFileHandle(IndexComponent.POSTING_LISTS, indexContext);
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

        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, indexContext, false))
        {
            long summaryPosition = writer.write(postingList);
            writer.complete();

            return summaryPosition;
        }
    }

    protected final PostingsReader openPostingsReader() throws IOException
    {
        IndexInput input = IndexFileUtils.instance.openInput(postings);
        IndexInput summaryInput = IndexFileUtils.instance.openInput(postings);

        PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryPosition);
        return new PostingsReader(input, summary, QueryEventListener.PostingListEventListener.NO_OP);
    }

    private void writeSSTableComponents(int rows) throws IOException
    {
        SSTableComponentsWriter writer = new SSTableComponentsWriter(indexDescriptor);
        for (int i = 0; i < rows; i++)
            writer.recordCurrentTokenOffset(toToken(i), toOffset(i));

        writer.complete(Stopwatch.createStarted());
    }

    protected final LongArray openRowIdToTokenReader() throws IOException
    {
        MetadataSource source = MetadataSource.loadGroupMetadata(indexDescriptor);
        NumericValuesMeta tokensMeta = new NumericValuesMeta(source.get(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES)));
        return new BlockPackedReader(token, tokensMeta).open();
    }
}
