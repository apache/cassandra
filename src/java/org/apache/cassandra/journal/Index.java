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
package org.apache.cassandra.journal;

import javax.annotation.Nullable;

import org.apache.cassandra.utils.Closeable;

/**
 * Mapping of client supplied ids to in-segment offsets
 */
interface Index<K> extends Closeable
{
    /**
     * Update the index with a new entry with id and offset
     */
    void update(K id, int offset);

    /**
     * Look up offsets by id. It's possible, due to retries, for a segment
     * to contain the same record with the same id more than once, at
     * different offsets.
     *
     * @return the found offsets into the segment, if any; can be empty
     */
    int[] lookUp(K id);

    /**
     * Look up offsets by id. It's possible, due to retries, for a segment
     * to contain the same record with the same id more than once, at
     * different offsets. Return the first offset for provided record id, or -1 if none.
     *
     * @return the first offset into the segment, or -1 is none were found
     */
    int lookUpFirst(K id);

    /**
     * @return the first (smallest) id in the index
     */
    @Nullable
    K firstId();

    /**
     * @return the last (largest) id in the index
     */
    @Nullable
    K lastId();

    /**
     * Persist the index on disk to the file matching the desrcriptor.
     */
    void persist(Descriptor descriptor);

    /**
     * @return whether the id falls within lower/upper bounds of the index
     */
    default boolean mayContainId(K id, KeySupport<K> keySupport)
    {
        K firstId = firstId();
        K lastId = lastId();

        return null != firstId && null != lastId && keySupport.compare(id, firstId) >= 0 && keySupport.compare(id, lastId) <= 0;
    }
}
