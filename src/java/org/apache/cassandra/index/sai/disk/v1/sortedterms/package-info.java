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

/**
 * Space-efficient on-disk data structure for storing a sorted sequence of terms.
 * Provides efficient lookup of terms by their point id, as well as locating them by contents.
 * <p>
 * All the code in the package uses the following teminology:
 * <ul>
 *     <li>Term: arbitrary data provided by the user as a bunch of bytes. Terms can be of variable length.</li>
 *     <li>Point id: the ordinal position of a term in the sequence, 0-based.</li>
 * </ul>
 *
 * Terms are stored in <code>ByteComparable</code> strictly ascending order.
 * Duplicates are not allowed.
 *
 * <p>
 * The structure is immutable, i.e. cannot be modified nor appended after writing to disk is completed.
 * You build it by adding terms in the ascending order using
 * {@link org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsWriter}.
 * Once saved to disk, you can open it for lookups with
 * {@link org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsReader}.
 *
 * <p>
 * The data structure comprises the following components, each stored in a separate file:
 * <ul>
 *     <li>terms data, organized as a sequence of prefix-compressed blocks each storing a number of terms calculated
 *     from {@code org.apache.cassandra.index.sai.disk.v1.sortedterms.SortedTermsWriter#blockShift}</li>
 *     <li>a monotonic list of file offsets of the blocks; this component allows to quickly locate the block
 *     that contains the term with a given point id</li>
 * </ul>
 * </p>
 * <p>
 * The sorted terms data structure is used for the storage and reading of {@link org.apache.cassandra.index.sai.utils.PrimaryKey}s
 * by the {@link org.apache.cassandra.index.sai.disk.v1.SkinnyRowAwarePrimaryKeyMap} and {@link org.apache.cassandra.index.sai.disk.v1.WideRowAwarePrimaryKeyMap}.
 */
package org.apache.cassandra.index.sai.disk.v1.sortedterms;
