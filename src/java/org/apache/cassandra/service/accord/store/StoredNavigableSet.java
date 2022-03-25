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
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Consumer;

public class StoredNavigableSet<T extends Comparable<?>> extends AbstractStoredField
{
    private NavigableSet<T> set = null;
    private NavigableSet<T> view = null;
    private NavigableSet<T> additions = null;
    private NavigableSet<T> deletions = null;

    @Override
    public boolean equals(Object o)
    {
        preGet();
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoredNavigableSet<?> that = (StoredNavigableSet<?>) o;
        return Objects.equals(set, that.set);
    }

    @Override
    public int hashCode()
    {
        preGet();
        return Objects.hash(set);
    }

    public void unload()
    {
        preUnload();
        set = null;
        view = null;
        additions = null;
        deletions = null;
    }

    void setInternal(NavigableSet<T> set)
    {
        this.set = set;
        this.view = Collections.unmodifiableNavigableSet(set);
    }

    public void load(NavigableSet<T> set)
    {
        preLoad();
        setInternal(set);
    }

    public NavigableSet<T> getView()
    {
        preGet();
        return view;
    }

    public void blindAdd(T item)
    {
        preBlindChange();
        if (isLoaded())
            set.add(item);

        if (additions == null)
            additions = new TreeSet<>();

        additions.add(item);
        if (deletions != null)
            deletions.remove(item);
    }

    public void blindRemove(T item)
    {
        preBlindChange();
        if (isLoaded())
            set.remove(item);

        if (deletions == null)
            deletions = new TreeSet<>();

        deletions.add(item);
        if (additions != null)
            additions.remove(item);
    }

    public void clear()
    {
        clearModifiedFlag();
        preClear();
        setInternal(new TreeSet<>());
    }

    @Override
    public void clearModifiedFlag()
    {
        super.clearModifiedFlag();
        // TODO: clear instead?
        additions = null;
        deletions = null;
    }

    public boolean hasAdditions()
    {
        return !additions.isEmpty();
    }

    public void forEachAddition(Consumer<T> consumer)
    {
        if (additions != null)
            additions.forEach(consumer);
    }

    public void forEachDeletion(Consumer<T> consumer)
    {
        if (deletions != null)
            deletions.forEach(consumer);
    }
}
