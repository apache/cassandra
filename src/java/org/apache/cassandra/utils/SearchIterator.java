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
package org.apache.cassandra.utils;

public interface SearchIterator<K, V>
{
    public boolean hasNext();

    /**
     * Searches "forwards" (in direction of travel) in the iterator for the required key;
     * if this or any key greater has already been returned by the iterator, the method may
     * choose to return null, the correct or incorrect output, or fail an assertion.
     *
     * it is permitted to search past the end of the iterator, i.e. {@code !hasNext() => next(?) == null}
     *
     * @param key to search for
     * @return value associated with key, if present in direction of travel
     */
    public V next(K key);
}
