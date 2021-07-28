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

package org.apache.cassandra.test.microbench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTree.Dir;
import org.apache.cassandra.utils.btree.FullBTreeSearchIterator;
import org.apache.cassandra.utils.btree.LeafBTreeSearchIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 4, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(1)
@State(Scope.Benchmark)
public class BTreeSearchIteratorBench
{
    private final int btreeSize = 32;

    @Param({"TREE", "LEAF"})
    private String iteratorType;

    @Param({ "0",  "1",  "2",  "3", "16", "17", "18", "19",
            "24", "25", "26", "30", "31" })
    private int targetIdx;

    private final int cellSize = 1000;

    @Param({"ASC", "DESC"})
    private String dirParam;

    private Dir dir;
    private Object[] btree;
    private ArrayList<String> data;
    private ArrayList<String> nonExistData;

    private static ArrayList<String> seq(int count, int minCellSize)
    {
        ArrayList<String> ret = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
        {
            StringBuilder sb = new StringBuilder();
            while (sb.length() < minCellSize)
            {
                String uuid = UUID.randomUUID().toString();
                sb.append(uuid);
            }
            ret.add(sb.toString());
        }
        Collections.sort(ret);
        return ret;
    }

    private static final Comparator<String> CMP = new Comparator<String>()
    {
        public int compare(String s1, String s2)
        {
            return s1.compareTo(s2);
        }
    };

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        data = seq(btreeSize, cellSize);
        nonExistData = new ArrayList<>();
        btree = BTree.build(data, UpdateFunction.noOp());
        for (String d : data)
        {
            nonExistData.add(d.substring(0, d.length() - 1) + "!");
        }
        dir = Dir.valueOf(dirParam);

    }

    @Benchmark
    public void searchFound()
    {
        BTreeSearchIterator<String, String> iter = getIterator();
        String val = iter.next(data.get(targetIdx));
        assert(val != null);
    }

    private BTreeSearchIterator<String,String> getIterator()
    {
        switch (iteratorType)
        {
            case "LEAF":
                return new LeafBTreeSearchIterator<>(btree, CMP, dir);
            case "TREE":
                return new FullBTreeSearchIterator<>(btree, CMP, dir);
            default:
                throw new IllegalArgumentException("unknown btree iterator type: " + iteratorType);
        }
    }

    @Benchmark
    public void searchNotFound()
    {
        BTreeSearchIterator<String, String> iter = getIterator();
        String val = iter.next(nonExistData.get(targetIdx));
        assert(val == null);
    }
}
