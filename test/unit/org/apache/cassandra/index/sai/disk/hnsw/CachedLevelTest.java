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

package org.apache.cassandra.index.sai.disk.hnsw;

import org.junit.Test;
import org.junit.Before;

import org.apache.cassandra.index.sai.disk.hnsw.OnDiskHnswGraph.CachedLevel;

import static org.assertj.core.api.Assertions.*;

public class CachedLevelTest {
    private CachedLevel cachedLevelWithNeighbors;
    private CachedLevel cachedLevelWithOffsets;

    @Before
    public void setup() {
        int[] nodeIds = new int[]{1, 2, 3};
        int[][] neighbors = new int[][]{
        new int[]{2, 3},
        new int[]{1, 3},
        new int[]{1, 2}
        };
        long[] offsets = new long[]{10L, 20L, 30L};

        cachedLevelWithNeighbors = new CachedLevel(1, nodeIds, neighbors);
        cachedLevelWithOffsets = new CachedLevel(1, nodeIds, offsets);
    }

    @Test
    public void testContainsNeighbors() {
        assertThat(cachedLevelWithNeighbors.containsNeighbors()).isTrue();
        assertThat(cachedLevelWithOffsets.containsNeighbors()).isFalse();
    }

    @Test
    public void testOffsetFor() {
        assertThat(cachedLevelWithOffsets.offsetFor(2)).isEqualTo(20L);
        assertThatThrownBy(() -> cachedLevelWithOffsets.offsetFor(5)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testNeighborsFor() {
        assertThat(cachedLevelWithNeighbors.neighborsFor(1)).containsExactly(2, 3);
        assertThatThrownBy(() -> cachedLevelWithNeighbors.neighborsFor(5)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testNodesOnLevel() {
        assertThat(cachedLevelWithNeighbors.nodesOnLevel()).containsExactly(1, 2, 3);
    }
}

