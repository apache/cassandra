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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.primitives.Ints;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.v1.MergePostingList;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Thread)
public class MergePostingListBenchmark
{
    List<int[]> splitPostingLists = new ArrayList<>();
    PostingList merge;

    @Setup(Level.Trial)
    public void generatePostings()
    {
        final AtomicInteger rowId = new AtomicInteger();
        final int[] postings = IntStream.generate(() -> rowId.addAndGet(7))
                                        .limit(1_000_000)
                                        .toArray();

        // split postings into multiple lists
        final Map<Integer, List<Integer>> splitPostings = Arrays.stream(postings)
                                                                .boxed()
                                                                .collect(Collectors.groupingBy(it -> it % 1000));

        for (List<Integer> split : splitPostings.values())
        {
            splitPostingLists.add(Ints.toArray(split));
        }
    }

    @Setup(Level.Invocation)
    public void mergePostings()
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = new PriorityQueue<>(Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        for (int[] postings : splitPostingLists)
        {
            lists.add(new ArrayPostingList(postings).peekable());
        }
        merge = MergePostingList.merge(lists);
    }

    @Benchmark
    @BenchmarkMode({ Mode.All })
    public void nextPostingIteration(Blackhole bh) throws IOException
    {
        long id;
        while ((id = merge.nextPosting()) != PostingList.END_OF_STREAM)
        {
            bh.consume(id);
        }
    }
}
