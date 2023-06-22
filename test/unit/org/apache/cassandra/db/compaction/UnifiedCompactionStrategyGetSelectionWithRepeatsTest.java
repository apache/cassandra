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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class UnifiedCompactionStrategyGetSelectionWithRepeatsTest extends UnifiedCompactionStrategyGetSelectionTest
{
    @Override
    boolean ignoreRepeats()
    {
        return false;
    }

    @Override
    List<CompactionSSTable> getSSTablesSet(List<List<CompactionSSTable>> sets, int levels, int perLevel, int level, int inLevel)
    {
        return sets.get(getRepeatIndex(levels * perLevel, level * perLevel + inLevel));
    }

    @Override
    List<List<CompactionSSTable>> prepareSSTablesSets(int levels, int perLevel)
    {
        return IntStream.range(0, levels * perLevel)
                        .mapToObj(i -> ImmutableList.of(Mockito.mock(CompactionSSTable.class)))
                        .collect(Collectors.toList());
    }

    private int getRepeatIndex(int size, int index)
    {
        double d = random.nextGaussian();
        if (d <= 0.5 || d > 1)
            return index;
        else
            return (int) (d * size - 1);    // high likelihood of hitting the same index
    }

}
