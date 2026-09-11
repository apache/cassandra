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
package org.apache.cassandra.index.sai.disk.v1.segment;

/**
 * Classifies a trie node posting as an exact match or an intermediate (prefix) posting.
 * The {@link #id} is the section index in {@link PackedLongValuesList}.
 * <p>
 * To add suffix search: add {@code SUFFIX(2)} here and set {@link PackedLongValuesList#FILTER_TYPES} to 3.
 */
public enum PostingType
{
    EXACT(0),   // terminal node — this term equals the indexed value
    PREFIX(1);  // intermediate node — this term is a prefix of the indexed value
    // SUFFIX(2) — reserved; add when suffix search is implemented

    public final int id;

    PostingType(int id)
    {
        this.id = id;
    }
}
