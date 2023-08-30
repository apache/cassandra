/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.v1.vector.hnsw;

import java.io.IOException;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * An {@link HnswGraph} that offers concurrent access; for typical graphs you will get significant
 * speedups in construction and searching as you add threads.
 *
 * <p>To search this graph, you should use a View obtained from {@link #getView()} to perform `seek`
 * and `nextNeighbor` operations.
 */
public final class ConcurrentOnHeapHnswGraph extends HnswGraph implements Accountable {
  private final AtomicReference<NodeAtLevel>
      entryPoint; // the current graph entry node on the top level. -1 if not set

  // Unlike OnHeapHnswGraph (OHHG), we use the same data structure for Level 0 and higher node
  // lists, a ConcurrentHashMap.  While the ArrayList used for L0 in OHHG is faster for
  // single-threaded workloads, it imposes an unacceptable contention burden for concurrent
  // graph building.
  private final Map<Integer, Map<Integer, ConcurrentNeighborSet>> graphLevels;
  private final CompletionTracker completions;

  // Neighbours' size on upper levels (nsize) and level 0 (nsize0)
  private final int nsize;
  private final int nsize0;

  ConcurrentOnHeapHnswGraph(int M) {
    this.entryPoint =
        new AtomicReference<>(
            new NodeAtLevel(0, -1)); // Entry node should be negative until a node is added
    this.nsize = M;
    this.nsize0 = 2 * M;

    this.graphLevels = new ConcurrentHashMap<>();
    this.completions = new CompletionTracker(nsize0);
  }

  /**
   * Returns the neighbors connected to the given node.
   *
   * @param level level of the graph
   * @param node the node whose neighbors are returned, represented as an ordinal on the level 0.
   */
  public ConcurrentNeighborSet getNeighbors(int level, int node) {
    return graphLevels.get(level).get(node);
  }

  @Override
  public int size() {
    Map<Integer, ConcurrentNeighborSet> levelZero = graphLevels.get(0);
    return levelZero == null ? 0 : levelZero.size(); // all nodes are located on the 0th level
  }

  @Override
  public void addNode(int level, int node) {
    if (level >= graphLevels.size()) {
      for (int i = graphLevels.size(); i <= level; i++) {
        graphLevels.putIfAbsent(i, new ConcurrentHashMap<>());
      }
    }

    graphLevels.get(level).put(node, new ConcurrentNeighborSet(node, connectionsOnLevel(level)));
  }

  /** must be called after addNode once neighbors are linked in all levels. */
  void markComplete(int level, int node) {
    entryPoint.accumulateAndGet(
        new NodeAtLevel(level, node),
        (oldEntry, newEntry) -> {
          if (oldEntry.node >= 0 && oldEntry.level >= level) {
            return oldEntry;
          } else {
            return newEntry;
          }
        });
    completions.markComplete(node);
  }

  private int connectionsOnLevel(int level) {
    return level == 0 ? nsize0 : nsize;
  }

  @Override
  public void seek(int level, int target) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nextNeighbor() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @return the current number of levels in the graph where nodes have been added and we have a
   *     valid entry point.
   */
  @Override
  public int numLevels() {
    return entryPoint.get().level + 1;
  }

  /**
   * Returns the graph's current entry node on the top level shown as ordinals of the nodes on 0th
   * level
   *
   * @return the graph's current entry node on the top level
   */
  @Override
  public int entryNode() {
    return entryPoint.get().node;
  }

  NodeAtLevel entry() {
    return entryPoint.get();
  }

  @Override
  public NodesIterator getNodesOnLevel(int level) {
    // We avoid the temptation to optimize L0 by using ArrayNodesIterator.
    // This is because, while L0 will contain sequential ordinals once the graph is complete,
    // and internally Lucene only calls getNodesOnLevel at that point, this is a public
    // method so we cannot assume that that is the only time it will be called by third parties.
    return new CollectionNodesIterator(graphLevels.get(level).keySet());
  }

  @Override
  public long ramBytesUsed() {
    // the main graph structure
    long total = concurrentHashMapRamUsed(graphLevels.size());
    for (int l = 0; l <= entryPoint.get().level; l++) {
      Map<Integer, ConcurrentNeighborSet> level = graphLevels.get(l);
      if (level == null) {
        continue;
      }

      int numNodesOnLevel = graphLevels.get(l).size();
      long chmSize = concurrentHashMapRamUsed(numNodesOnLevel);
      long neighborSize = neighborsRamUsed(connectionsOnLevel(l)) * numNodesOnLevel;

      total += chmSize + neighborSize;
    }

    // logical clocks
    total += completions.ramBytesUsed();

    return total;
  }

  public long ramBytesUsedOneNode(int nodeLevel) {
    int entryCount = (int) (nodeLevel / CHM_LOAD_FACTOR);
    var graphBytesUsed =
        chmEntriesRamUsed(entryCount)
            + neighborsRamUsed(connectionsOnLevel(0))
            + nodeLevel * neighborsRamUsed(connectionsOnLevel(1));
    var clockBytesUsed = Integer.BYTES;
    return graphBytesUsed + clockBytesUsed;
  }

  private static long neighborsRamUsed(int count) {
    long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    long neighborSetBytes =
        REF_BYTES // atomicreference
            + Integer.BYTES
            + Integer.BYTES
            + REF_BYTES // NeighborArray
            + AH_BYTES * 2
            + REF_BYTES * 2
            + Integer.BYTES
            + 1; // NeighborArray internals
    return neighborSetBytes + (long) count * (Integer.BYTES + Float.BYTES);
  }

  private static final float CHM_LOAD_FACTOR = 0.75f; // this is hardcoded inside ConcurrentHashMap

  /**
   * caller's responsibility to divide number of entries by load factor to get internal node count
   */
  private static long chmEntriesRamUsed(int internalEntryCount) {
    long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    long chmNodeBytes =
        REF_BYTES // node itself in Node[]
            + 3L * REF_BYTES
            + Integer.BYTES; // node internals

    return internalEntryCount * chmNodeBytes;
  }

  private static long concurrentHashMapRamUsed(int externalSize) {
    long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    long AH_BYTES = RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    long CORES = Runtime.getRuntime().availableProcessors();

    // CHM has a striped counter Cell implementation, we expect at most one per core
    long chmCounters = AH_BYTES + CORES * (REF_BYTES + Long.BYTES);

    int nodeCount = (int) (externalSize / CHM_LOAD_FACTOR);

    long chmSize =
        chmEntriesRamUsed(nodeCount) // nodes
            + nodeCount * REF_BYTES
            + AH_BYTES // nodes array
            + Long.BYTES
            + 3 * Integer.BYTES
            + 3 * REF_BYTES // extra internal fields
            + chmCounters
            + REF_BYTES; // the Map reference itself
    return chmSize;
  }

  @Override
  public String toString() {
    return "ConcurrentOnHeapHnswGraph(size=" + size() + ", entryPoint=" + entryPoint.get();
  }

  /**
   * Returns a view of the graph that is safe to use concurrently with updates performed on the
   * underlying graph.
   *
   * <p>Multiple Views may be searched concurrently.
   */
  public HnswGraph getView() {
    return new ConcurrentHnswGraphView();
  }

  void validateEntryNode() {
    if (size() == 0) {
      return;
    }
    var en = entryPoint.get();
    if (!(en.level >= 0 && en.node >= 0 && graphLevels.get(en.level).containsKey(en.node))) {
      throw new IllegalStateException("Entry node was incompletely added! " + en);
    }
  }

  /**
   * A concurrent View of the graph that is safe to search concurrently with updates and with other
   * searches. The View provides a limited kind of snapshot isolation: only nodes completely added
   * to the graph at the time the View was created will be visible (but the connections between them
   * are allowed to change, so you could potentially get different top K results from the same query
   * if concurrent updates are in progress.)
   */
  private class ConcurrentHnswGraphView extends HnswGraph {
    // It is tempting, but incorrect, to try to provide "adequate" isolation by
    // (1) keeping a bitset of complete nodes and giving that to the searcher as nodes to
    // accept -- but we need to keep incomplete nodes out of the search path entirely,
    // not just out of the result set, or
    // (2) keeping a bitset of complete nodes and restricting the View to those nodes
    // -- but we needs to consider neighbor diversity separately for concurrent
    // inserts and completed nodes; this allows us to keep the former out of the latter,
    // but not the latter out of the former (when a node completes while we are working,
    // that was in-progress when we started.)
    // The only really foolproof solution is to implement snapshot isolation as
    // we have done here.
    private final int timestamp;
    private PrimitiveIterator.OfInt remainingNeighbors;

    public ConcurrentHnswGraphView() {
      this.timestamp = completions.clock();
    }

    @Override
    public int size() {
      return ConcurrentOnHeapHnswGraph.this.size();
    }

    @Override
    public int numLevels() {
      return ConcurrentOnHeapHnswGraph.this.numLevels();
    }

    @Override
    public int entryNode() {
      return ConcurrentOnHeapHnswGraph.this.entryNode();
    }

    @Override
    public NodesIterator getNodesOnLevel(int level) {
      return ConcurrentOnHeapHnswGraph.this.getNodesOnLevel(level);
    }

    @Override
    public void seek(int level, int targetNode) {
      remainingNeighbors = getNeighbors(level, targetNode).nodeIterator();
    }

    @Override
    public int nextNeighbor() {
      while (remainingNeighbors.hasNext()) {
        int next = remainingNeighbors.nextInt();
        if (completions.completedAt(next) < timestamp) {
          return next;
        }
      }
      return NO_MORE_DOCS;
    }

    @Override
    public String toString() {
      return "ConcurrentOnHeapHnswGraphView(size=" + size() + ", entryPoint=" + entryPoint.get();
    }
  }

  static final class NodeAtLevel implements Comparable<NodeAtLevel> {
    public final int level;
    public final int node;

    public NodeAtLevel(int level, int node) {
      this.level = level;
      this.node = node;
    }

    @Override
    public int compareTo(NodeAtLevel o) {
      int cmp = Integer.compare(level, o.level);
      if (cmp == 0) {
        cmp = Integer.compare(node, o.node);
      }
      return cmp;
    }

    @Override
    public String toString() {
      return "NodeAtLevel(level=" + level + ", node=" + node + ")";
    }
  }

  /** Class to provide snapshot isolation for nodes in the progress of being added. */
  static final class CompletionTracker implements Accountable {
    private final AtomicInteger logicalClock = new AtomicInteger();
    private volatile AtomicIntegerArray completionTimes;
    private final StampedLock sl = new StampedLock();

    public CompletionTracker(int initialSize) {
      completionTimes = new AtomicIntegerArray(initialSize);
      for (int i = 0; i < initialSize; i++) {
        completionTimes.set(i, Integer.MAX_VALUE);
      }
    }

    /**
     * @param node ordinal
     */
    void markComplete(int node) {
      int completionClock = logicalClock.getAndIncrement();
      ensureCapacity(node);
      long stamp;
      do {
        stamp = sl.tryOptimisticRead();
        completionTimes.set(node, completionClock);
      } while (!sl.validate(stamp));
    }

    /**
     * @return the current logical timestamp; can be compared with completedAt values
     */
    int clock() {
      return logicalClock.get();
    }

    /**
     * @param node ordinal
     * @return the logical clock completion time of the node, or Integer.MAX_VALUE if the node has
     *     not yet been completed.
     */
    public int completedAt(int node) {
      AtomicIntegerArray ct = completionTimes;
      if (node >= ct.length()) {
        return Integer.MAX_VALUE;
      }
      return ct.get(node);
    }

    @Override
    public long ramBytesUsed() {
      int REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
      return REF_BYTES
          + Integer.BYTES // logicalClock
          + REF_BYTES
          + (long) Integer.BYTES * completionTimes.length();
    }

    private void ensureCapacity(int node) {
      if (node < completionTimes.length()) {
        return;
      }

      long stamp = sl.writeLock();
      try {
        AtomicIntegerArray oldArray = completionTimes;
        if (node >= oldArray.length()) {
          int newSize = (node + 1) * 2;
          AtomicIntegerArray newArray = new AtomicIntegerArray(newSize);
          for (int i = 0; i < newSize; i++) {
            if (i < oldArray.length()) {
              newArray.set(i, oldArray.get(i));
            } else {
              newArray.set(i, Integer.MAX_VALUE);
            }
          }
          completionTimes = newArray;
        }
      } finally {
        sl.unlockWrite(stamp);
      }
    }
  }
}
