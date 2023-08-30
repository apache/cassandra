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

import java.util.Arrays;

import org.apache.lucene.util.ArrayUtil;

/**
 * NeighborArray encodes the neighbors of a node and their mutual scores in the HNSW graph as a pair
 * of growable arrays. Nodes are arranged in the sorted order of their scores in descending order
 * (if scoresDescOrder is true), or in the ascending order of their scores (if scoresDescOrder is
 * false)
 *
 * @lucene.internal
 */
public class NeighborArray {
  protected final boolean scoresDescOrder;
  protected int size;

  float[] score;
  int[] node;

  public NeighborArray(int maxSize, boolean descOrder) {
    node = new int[maxSize];
    score = new float[maxSize];
    this.scoresDescOrder = descOrder;
  }

  /**
   * Add a new node to the NeighborArray. The new node must be worse than all previously stored
   * nodes.
   */
  public void add(int newNode, float newScore) {
    if (size == node.length) {
      growArrays();
    }
    if (size > 0) {
      float previousScore = score[size - 1];
      assert ((scoresDescOrder && (previousScore >= newScore))
              || (scoresDescOrder == false && (previousScore <= newScore)))
          : "Nodes are added in the incorrect order!";
    }
    node[size] = newNode;
    score[size] = newScore;
    ++size;
  }

  /** Add a new node to the NeighborArray into a correct sort position according to its score. */
  public void insertSorted(int newNode, float newScore) {
    if (size == node.length) {
      growArrays();
    }
    int insertionPoint =
        scoresDescOrder
            ? descSortFindRightMostInsertionPoint(newScore)
            : ascSortFindRightMostInsertionPoint(newScore);
    System.arraycopy(node, insertionPoint, node, insertionPoint + 1, size - insertionPoint);
    System.arraycopy(score, insertionPoint, score, insertionPoint + 1, size - insertionPoint);
    node[insertionPoint] = newNode;
    score[insertionPoint] = newScore;
    ++size;
  }

  protected void growArrays() {
    node = ArrayUtil.grow(node);
    score = ArrayUtil.growExact(score, node.length);
  }

  public int size() {
    return size;
  }

  /**
   * Direct access to the internal list of node ids; provided for efficient writing of the graph
   *
   * @lucene.internal
   */
  public int[] node() {
    return node;
  }

  public float[] score() {
    return score;
  }

  public void clear() {
    size = 0;
  }

  public void removeLast() {
    size--;
  }

  public void removeIndex(int idx) {
    System.arraycopy(node, idx + 1, node, idx, size - idx - 1);
    System.arraycopy(score, idx + 1, score, idx, size - idx - 1);
    size--;
  }

  @Override
  public String toString() {
    return "NeighborArray[" + size + "]";
  }

  protected int ascSortFindRightMostInsertionPoint(float newScore) {
    int insertionPoint = Arrays.binarySearch(score, 0, size, newScore);
    if (insertionPoint >= 0) {
      // find the right most position with the same score
      while ((insertionPoint < size - 1) && (score[insertionPoint + 1] == score[insertionPoint])) {
        insertionPoint++;
      }
      insertionPoint++;
    } else {
      insertionPoint = -insertionPoint - 1;
    }
    return insertionPoint;
  }

  protected int descSortFindRightMostInsertionPoint(float newScore) {
    int start = 0;
    int end = size - 1;
    while (start <= end) {
      int mid = (start + end) / 2;
      if (score[mid] < newScore) end = mid - 1;
      else start = mid + 1;
    }
    return start;
  }
}
