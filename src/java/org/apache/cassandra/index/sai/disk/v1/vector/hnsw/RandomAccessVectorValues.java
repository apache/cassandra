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

/**
 * Provides random access to vectors by dense ordinal. This interface is used by HNSW-based
 * implementations of KNN search.
 *
 * @lucene.experimental
 */
public interface RandomAccessVectorValues<T> {

  /** Return the number of vector values */
  int size();

  /** Return the dimension of the returned vector values */
  int dimension();

  /**
   * Return the vector value indexed at the given ordinal.
   *
   * @param targetOrd a valid ordinal, &ge; 0 and &lt; {@link #size()}.
   */
  T vectorValue(int targetOrd) throws IOException;

  /**
   * Creates a new copy of this {@link RandomAccessVectorValues}. This is helpful when you need to
   * access different values at once, to avoid overwriting the underlying float vector returned by
   * {@link RandomAccessVectorValues#vectorValue}.
   */
  RandomAccessVectorValues<T> copy() throws IOException;
}
