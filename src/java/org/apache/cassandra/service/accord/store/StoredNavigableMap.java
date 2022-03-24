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

package org.apache.cassandra.service.accord.store;

import java.util.Collections;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Navigable Map, capable of blind add/remove
 */
public class StoredNavigableMap<K, V> extends AbstractStoredField
{
    private NavigableMap<K, V> map = null;
    private NavigableMap<K, V> view = null;
    private NavigableMap<K, V> additions = null;
    private NavigableSet<K> deletions = null;

    public void unload()
    {
        preUnload();
        map = null;
        view = null;
        additions = null;
        deletions = null;
    }

    public void load(NavigableMap<K, V> map)
    {
        preLoad();
        this.map = map;
        this.view = Collections.unmodifiableNavigableMap(map);
    }

    public NavigableMap<K, V> getView()
    {
        preGet();
        return view;
    }

    public void blindPut(K key, V val)
    {
        preBlindChange();
        if (isLoaded())
            map.put(key, val);

        if (additions == null)
            additions = new TreeMap<>();

        additions.put(key, val);
    }

    public void blindRemove(K key)
    {
        preBlindChange();
        if (isLoaded())
            map.remove(key);

        if (deletions == null)
            deletions = new TreeSet<>();

        deletions.add(key);
    }

    @Override
    public void clearModifiedFlag()
    {
        super.clearModifiedFlag();
        // TODO: clear instead?
        additions = null;
        deletions = null;
    }
}
