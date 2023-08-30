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
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Clock;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InfoStream;

import static java.lang.Math.log;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Builder for HNSW graph. See {@link HnswGraph} for a gloss on the algorithm and the meaning of the
 * hyperparameters.
 *
 * @param <T> the type of vector
 */
public final class HnswGraphBuilder<T> {

  /** Default number of maximum connections per node */
  public static final int DEFAULT_MAX_CONN = 16;

  /**
   * Default number of the size of the queue maintained while searching during a graph construction.
   */
  public static final int DEFAULT_BEAM_WIDTH = 100;

  /** Default random seed for level generation * */
  private static final long DEFAULT_RAND_SEED = 42;

  /** A name for the HNSW component for the info-stream * */
  public static final String HNSW_COMPONENT = "HNSW";

  /** Random seed for level generation; public to expose for testing * */
  public static long randSeed = DEFAULT_RAND_SEED;

  private final int M; // max number of connections on upper layers
  private final int beamWidth;
  private final double ml;
  private final NeighborArray scratch;

  private final VectorSimilarityFunction similarityFunction;
  private final VectorEncoding vectorEncoding;
  private final RandomAccessVectorValues<T> vectors;
  private final SplittableRandom random;
  private final HnswGraphSearcher<T> graphSearcher;
  private final NeighborQueue entryCandidates; // for upper levels of graph search
  private final NeighborQueue beamCandidates; // for levels of graph where we add the node

  final OnHeapHnswGraph hnsw;

  private InfoStream infoStream = InfoStream.getDefault();

  // we need two sources of vectors in order to perform diversity check comparisons without
  // colliding
  private final RandomAccessVectorValues<T> vectorsCopy;
  // tracks nodes that were already added when initializing from another graph
  private final Set<Integer> initializedNodes;

  public static <T> HnswGraphBuilder<T> create(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed)
      throws IOException {
    return new HnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, M, beamWidth, seed);
  }

  public static <T> HnswGraphBuilder<T> create(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed,
      HnswGraph initializerGraph,
      Map<Integer, Integer> oldToNewOrdinalMap)
      throws IOException {
    HnswGraphBuilder<T> hnswGraphBuilder =
        new HnswGraphBuilder<>(vectors, vectorEncoding, similarityFunction, M, beamWidth, seed);
    hnswGraphBuilder.initializeFromGraph(initializerGraph, oldToNewOrdinalMap);
    return hnswGraphBuilder;
  }

  /**
   * Reads all the vectors from vector values, builds a graph connecting them by their dense
   * ordinals, using the given hyperparameter settings, and returns the resulting graph.
   *
   * @param vectors the vectors whose relations are represented by the graph - must provide a
   *     different view over those vectors than the one used to add via addGraphNode.
   * @param M – graph fanout parameter used to calculate the maximum number of connections a node
   *     can have – M on upper layers, and M * 2 on the lowest level.
   * @param beamWidth the size of the beam search to use when finding nearest neighbors.
   * @param seed the seed for a random number generator used during graph construction. Provide this
   *     to ensure repeatable construction.
   */
  private HnswGraphBuilder(
      RandomAccessVectorValues<T> vectors,
      VectorEncoding vectorEncoding,
      VectorSimilarityFunction similarityFunction,
      int M,
      int beamWidth,
      long seed)
      throws IOException {
    this.vectors = vectors;
    this.vectorsCopy = vectors.copy();
    this.vectorEncoding = Objects.requireNonNull(vectorEncoding);
    this.similarityFunction = Objects.requireNonNull(similarityFunction);
    if (M <= 0) {
      throw new IllegalArgumentException("maxConn must be positive");
    }
    if (beamWidth <= 0) {
      throw new IllegalArgumentException("beamWidth must be positive");
    }
    this.M = M;
    this.beamWidth = beamWidth;
    // normalization factor for level generation; currently not configurable
    this.ml = M == 1 ? 1 : 1 / Math.log(1.0 * M);
    this.random = new SplittableRandom(seed);
    this.hnsw = new OnHeapHnswGraph(M);
    this.graphSearcher =
        new HnswGraphSearcher<>(
            vectorEncoding,
            similarityFunction,
            new NeighborQueue(beamWidth, true),
            new FixedBitSet(this.vectors.size()));
    // in scratch we store candidates in reverse order: worse candidates are first
    scratch = new NeighborArray(Math.max(beamWidth, M + 1), false);
    entryCandidates = new NeighborQueue(1, false);
    beamCandidates = new NeighborQueue(beamWidth, false);
    this.initializedNodes = new HashSet<>();
  }

  /**
   * Reads all the vectors from two copies of a {@link RandomAccessVectorValues}. Providing two
   * copies enables efficient retrieval without extra data copying, while avoiding collision of the
   * returned values.
   *
   * @param vectorsToAdd the vectors for which to build a nearest neighbors graph. Must be an
   *     independent accessor for the vectors
   */
  public OnHeapHnswGraph build(RandomAccessVectorValues<T> vectorsToAdd) throws IOException {
    if (vectorsToAdd == this.vectors) {
      throw new IllegalArgumentException(
          "Vectors to build must be independent of the source of vectors provided to HnswGraphBuilder()");
    }
    if (infoStream.isEnabled(HNSW_COMPONENT)) {
      infoStream.message(HNSW_COMPONENT, "build graph from " + vectorsToAdd.size() + " vectors");
    }
    addVectors(vectorsToAdd);
    return hnsw;
  }

  /**
   * Initializes the graph of this builder. Transfers the nodes and their neighbors from the
   * initializer graph into the graph being produced by this builder, mapping ordinals from the
   * initializer graph to their new ordinals in this builder's graph. The builder's graph must be
   * empty before calling this method.
   *
   * @param initializerGraph graph used for initialization
   * @param oldToNewOrdinalMap map for converting from ordinals in the initializerGraph to this
   *     builder's graph
   */
  private void initializeFromGraph(
      HnswGraph initializerGraph, Map<Integer, Integer> oldToNewOrdinalMap) throws IOException {
    assert hnsw.size() == 0;
    float[] vectorValue = null;
    byte[] binaryValue = null;
    for (int level = 0; level < initializerGraph.numLevels(); level++) {
      HnswGraph.NodesIterator it = initializerGraph.getNodesOnLevel(level);

      while (it.hasNext()) {
        int oldOrd = it.nextInt();
        int newOrd = oldToNewOrdinalMap.get(oldOrd);

        hnsw.addNode(level, newOrd);

        if (level == 0) {
          initializedNodes.add(newOrd);
        }

        switch (this.vectorEncoding) {
          case FLOAT32:
            vectorValue = (float[]) vectors.vectorValue(newOrd);
            break;
          case BYTE:
            binaryValue = (byte[]) vectors.vectorValue(newOrd);
            break;
        }

        NeighborArray newNeighbors = this.hnsw.getNeighbors(level, newOrd);
        initializerGraph.seek(level, oldOrd);
        for (int oldNeighbor = initializerGraph.nextNeighbor();
            oldNeighbor != NO_MORE_DOCS;
            oldNeighbor = initializerGraph.nextNeighbor()) {
          int newNeighbor = oldToNewOrdinalMap.get(oldNeighbor);
          float score;
          switch (this.vectorEncoding) {
            case FLOAT32:
            default:
              score =
                  this.similarityFunction.compare(
                      vectorValue, (float[]) vectorsCopy.vectorValue(newNeighbor));
              break;
            case BYTE:
              score =
                  this.similarityFunction.compare(
                      binaryValue, (byte[]) vectorsCopy.vectorValue(newNeighbor));
              break;
          }
          newNeighbors.insertSorted(newNeighbor, score);
        }
      }
    }
  }

  private void addVectors(RandomAccessVectorValues<T> vectorsToAdd) throws IOException {
    long start = Clock.Global.nanoTime(), t = start;
    for (int node = 0; node < vectorsToAdd.size(); node++) {
      if (initializedNodes.contains(node)) {
        continue;
      }
      addGraphNode(node, vectorsToAdd);
      if ((node % 10000 == 0) && infoStream.isEnabled(HNSW_COMPONENT)) {
        t = printGraphBuildStatus(node, start, t);
      }
    }
  }

  /** Set info-stream to output debugging information * */
  public void setInfoStream(InfoStream infoStream) {
    this.infoStream = infoStream;
  }

  public OnHeapHnswGraph getGraph() {
    return hnsw;
  }

  /** Inserts a doc with vector value to the graph */
  public void addGraphNode(int node, T value) throws IOException {
    final int nodeLevel = getRandomGraphLevel(ml, random);
    int curMaxLevel = hnsw.numLevels() - 1;

    // If entrynode is -1, then this should finish without adding neighbors
    if (hnsw.entryNode() == -1) {
      for (int level = nodeLevel; level >= 0; level--) {
        hnsw.addNode(level, node);
      }
      return;
    }
    int[] eps = new int[] {hnsw.entryNode()};

    // if a node introduces new levels to the graph, add this new node on new levels
    for (int level = nodeLevel; level > curMaxLevel; level--) {
      hnsw.addNode(level, node);
    }

    // for levels > nodeLevel search with topk = 1
    NeighborQueue candidates = entryCandidates;
    for (int level = curMaxLevel; level > nodeLevel; level--) {
      candidates.clear();
      graphSearcher.searchLevel(
          candidates, value, 1, level, eps, vectors, hnsw, null, Integer.MAX_VALUE);
      eps = new int[] {candidates.pop()};
    }
    // for levels <= nodeLevel search with topk = beamWidth, and add connections
    candidates = beamCandidates;
    for (int level = Math.min(nodeLevel, curMaxLevel); level >= 0; level--) {
      candidates.clear();
      graphSearcher.searchLevel(
          candidates, value, beamWidth, level, eps, vectors, hnsw, null, Integer.MAX_VALUE);
      eps = candidates.nodes();
      hnsw.addNode(level, node);
      addDiverseNeighbors(level, node, candidates);
    }
  }

  public void addGraphNode(int node, RandomAccessVectorValues<T> values) throws IOException {
    addGraphNode(node, values.vectorValue(node));
  }

  private long printGraphBuildStatus(int node, long start, long t) {
    long now = Clock.Global.nanoTime();
    infoStream.message(
        HNSW_COMPONENT,
        String.format(
            Locale.ROOT,
            "built %d in %d/%d ms",
            node,
            TimeUnit.NANOSECONDS.toMillis(now - t),
            TimeUnit.NANOSECONDS.toMillis(now - start)));
    return now;
  }

  private void addDiverseNeighbors(int level, int node, NeighborQueue candidates)
      throws IOException {
    /* For each of the beamWidth nearest candidates (going from best to worst), select it only if it
     * is closer to target than it is to any of the already-selected neighbors (ie selected in this method,
     * since the node is new and has no prior neighbors).
     */
    NeighborArray neighbors = hnsw.getNeighbors(level, node);
    assert neighbors.size() == 0; // new node
    popToScratch(candidates);
    int maxConnOnLevel = level == 0 ? M * 2 : M;
    selectAndLinkDiverse(neighbors, scratch, maxConnOnLevel);

    // Link the selected nodes to the new node, and the new node to the selected nodes (again
    // applying diversity heuristic)
    int size = neighbors.size();
    for (int i = 0; i < size; i++) {
      int nbr = neighbors.node[i];
      NeighborArray nbrNbr = hnsw.getNeighbors(level, nbr);
      nbrNbr.insertSorted(node, neighbors.score[i]);
      if (nbrNbr.size() > maxConnOnLevel) {
        int indexToRemove = findWorstNonDiverse(nbrNbr);
        nbrNbr.removeIndex(indexToRemove);
      }
    }
  }

  private void selectAndLinkDiverse(
      NeighborArray neighbors, NeighborArray candidates, int maxConnOnLevel) throws IOException {
    // Select the best maxConnOnLevel neighbors of the new node, applying the diversity heuristic
    for (int i = candidates.size() - 1; neighbors.size() < maxConnOnLevel && i >= 0; i--) {
      // compare each neighbor (in distance order) against the closer neighbors selected so far,
      // only adding it if it is closer to the target than to any of the other selected neighbors
      int cNode = candidates.node[i];
      float cScore = candidates.score[i];
      assert cNode < hnsw.size();
      if (diversityCheck(cNode, cScore, neighbors)) {
        neighbors.add(cNode, cScore);
      }
    }
  }

  private void popToScratch(NeighborQueue candidates) {
    scratch.clear();
    int candidateCount = candidates.size();
    // extract all the Neighbors from the queue into an array; these will now be
    // sorted from worst to best
    for (int i = 0; i < candidateCount; i++) {
      float maxSimilarity = candidates.topScore();
      scratch.add(candidates.pop(), maxSimilarity);
    }
  }

  /**
   * @param candidate the vector of a new candidate neighbor of a node n
   * @param score the score of the new candidate and node n, to be compared with scores of the
   *     candidate and n's neighbors
   * @param neighbors the neighbors selected so far
   * @return whether the candidate is diverse given the existing neighbors
   */
  private boolean diversityCheck(int candidate, float score, NeighborArray neighbors)
      throws IOException {
    return isDiverse(candidate, neighbors, score);
  }

  private boolean isDiverse(int candidate, NeighborArray neighbors, float score)
      throws IOException {
    switch (vectorEncoding) {
      case BYTE:
        return isDiverse((byte[]) vectors.vectorValue(candidate), neighbors, score);
      default:
      case FLOAT32:
        return isDiverse((float[]) vectors.vectorValue(candidate), neighbors, score);
    }
  }

  private boolean isDiverse(float[] candidate, NeighborArray neighbors, float score)
      throws IOException {
    for (int i = 0; i < neighbors.size(); i++) {
      float neighborSimilarity =
          similarityFunction.compare(
              candidate, (float[]) vectorsCopy.vectorValue(neighbors.node[i]));
      if (neighborSimilarity >= score) {
        return false;
      }
    }
    return true;
  }

  private boolean isDiverse(byte[] candidate, NeighborArray neighbors, float score)
      throws IOException {
    for (int i = 0; i < neighbors.size(); i++) {
      float neighborSimilarity =
          similarityFunction.compare(
              candidate, (byte[]) vectorsCopy.vectorValue(neighbors.node[i]));
      if (neighborSimilarity >= score) {
        return false;
      }
    }
    return true;
  }

  /**
   * Find first non-diverse neighbour among the list of neighbors starting from the most distant
   * neighbours
   */
  private int findWorstNonDiverse(NeighborArray neighbors) throws IOException {
    for (int i = neighbors.size() - 1; i > 0; i--) {
      if (isWorstNonDiverse(i, neighbors)) {
        return i;
      }
    }
    return neighbors.size() - 1;
  }

  private boolean isWorstNonDiverse(int candidateIndex, NeighborArray neighbors)
      throws IOException {
    int candidateNode = neighbors.node[candidateIndex];
    switch (vectorEncoding) {
      case BYTE:
        return isWorstNonDiverse(
            candidateIndex, (byte[]) vectors.vectorValue(candidateNode), neighbors);
      default:
      case FLOAT32:
        return isWorstNonDiverse(
            candidateIndex, (float[]) vectors.vectorValue(candidateNode), neighbors);
    }
  }

  private boolean isWorstNonDiverse(
      int candidateIndex, float[] candidateVector, NeighborArray neighbors) throws IOException {
    float minAcceptedSimilarity = neighbors.score[candidateIndex];
    for (int i = candidateIndex - 1; i >= 0; i--) {
      float neighborSimilarity =
          similarityFunction.compare(
              candidateVector, (float[]) vectorsCopy.vectorValue(neighbors.node[i]));
      // candidate node is too similar to node i given its score relative to the base node
      if (neighborSimilarity >= minAcceptedSimilarity) {
        return true;
      }
    }
    return false;
  }

  private boolean isWorstNonDiverse(
      int candidateIndex, byte[] candidateVector, NeighborArray neighbors) throws IOException {
    float minAcceptedSimilarity = neighbors.score[candidateIndex];
    for (int i = candidateIndex - 1; i >= 0; i--) {
      float neighborSimilarity =
          similarityFunction.compare(
              candidateVector, (byte[]) vectorsCopy.vectorValue(neighbors.node[i]));
      // candidate node is too similar to node i given its score relative to the base node
      if (neighborSimilarity >= minAcceptedSimilarity) {
        return true;
      }
    }
    return false;
  }

  private static int getRandomGraphLevel(double ml, SplittableRandom random) {
    double randDouble;
    do {
      randDouble = random.nextDouble(); // avoid 0 value, as log(0) is undefined
    } while (randDouble == 0.0);
    return ((int) (-log(randDouble) * ml));
  }
}
