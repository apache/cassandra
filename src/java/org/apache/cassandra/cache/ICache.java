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
package org.apache.cassandra.cache;

import java.util.Set;

/**
 * This is similar to the Map interface, but requires maintaining a given capacity
 * and does not require put or remove to return values, which lets SerializingCache
 * be more efficient by avoiding deserialize except on get.
 */
public interface ICache<K, V>
{
    public long capacity();

    public void setCapacity(long capacity);

    public void put(K key, V value);

    public boolean putIfAbsent(K key, V value);

    public boolean replace(K key, V old, V value);

    public V get(K key);

    public void remove(K key);

    public int size();

    public long weightedSize();

    public void clear();

    public Set<K> keySet();

    public Set<K> hotKeySet(int n);

    public boolean containsKey(K key);
}
