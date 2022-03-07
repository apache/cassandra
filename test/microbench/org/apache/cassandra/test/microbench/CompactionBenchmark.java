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


import java.util.Map;

import org.apache.cassandra.db.compaction.CompactionStrategyFactory;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;

public class CompactionBenchmark extends BaseCompactionBenchmark
{
    @Param({"false", "true"})
    boolean cursors;

    @Benchmark
    public void compactSSTables() throws Throwable
    {
        cfs.forceMajorCompaction();
    }

    @Override
    public String compactionClass()
    {
        return cursors ? "SizeTieredCompactionStrategy" : "org.apache.cassandra.test.microbench.CompactionBenchmark$CursorDisabledStrategy";
    }

    public static class CursorDisabledStrategy extends SizeTieredCompactionStrategy
    {
        public CursorDisabledStrategy(CompactionStrategyFactory factory, Map<String, String> options)
        {
            super(factory, options);
        }

        public boolean supportsCursorCompaction()
        {
            return false;
        }
    }
}
