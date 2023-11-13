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

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 *
 * A variant of BiMap which does not enforce uniqueness of values. This means the inverse
 * is a Multimap.  (But the "forward" view is not a multimap; keys may only each have one value.)
 *
 * @param <K>
 * @param <V>
 */
public class BiMultiValMap<K, V> extends AbstractBiMultiValMap<K, V>
{
    protected final Map<K, V> forwardMap;
    protected final Multimap<V, K> reverseMap;

    public BiMultiValMap()
    {
        this.forwardMap = new HashMap<K, V>();
        this.reverseMap = HashMultimap.<V, K>create();
    }

    protected BiMultiValMap(Map<K, V> forwardMap, Multimap<V, K> reverseMap)
    {
        this.forwardMap = forwardMap;
        this.reverseMap = reverseMap;
    }

    public BiMultiValMap(BiMultiValMap<K, V> map)
    {
        this();
        forwardMap.putAll(map);
        reverseMap.putAll(map.inverse());
    }

    @Override
    protected Map<K, V> forwardDelegate()
    {
        return forwardMap;
    }

    @Override
    protected Multimap<V, K> reverseDelegate()
    {
        return reverseMap;
    }
}
