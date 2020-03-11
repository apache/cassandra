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

package org.apache.cassandra.distributed.api;

import java.util.Arrays;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;

/**
 * Data representing a single row in a query result.
 *
 * This class is mutable from the parent {@link QueryResult} and can have the row it points to changed between calls
 * to {@link QueryResult#hasNext()}, for this reason it is unsafe to hold reference to this class after that call;
 * to get around this, a call to {@link #copy()} will return a new object pointing to the same row.
 */
public class Row
{
    private final ObjectIntMap<String> nameIndex;
    @Nullable private Object[] results; // mutable to avoid allocations in loops

    public Row(String[] names)
    {
        Objects.requireNonNull(names, "names");
        this.nameIndex = new ObjectIntHashMap<>(names.length);
        for (int i = 0; i < names.length; i++) {
            nameIndex.put(names[i], i);
        }
    }

    private Row(ObjectIntMap<String> nameIndex)
    {
        this.nameIndex = nameIndex;
    }

    void setResults(@Nullable Object[] results)
    {
        this.results = results;
    }

    /**
     * Creates a copy of the current row; can be used past calls to {@link QueryResult#hasNext()}.
     */
    public Row copy() {
        Row copy = new Row(nameIndex);
        copy.setResults(results);
        return copy;
    }

    public <T> T get(String name)
    {
        checkAccess();
        int idx = findIndex(name);
        if (idx == -1)
            return null;
        return (T) results[idx];
    }

    public String getString(String name)
    {
        return get(name);
    }

    public UUID getUUID(String name)
    {
        return get(name);
    }

    public Date getTimestamp(String name)
    {
        return get(name);
    }

    public <T> Set<T> getSet(String name)
    {
        return get(name);
    }

    public String toString()
    {
        return "Row{" +
               "names=" + nameIndex.keys() +
               ", results=" + Arrays.toString(results) +
               '}';
    }

    private void checkAccess()
    {
        if (results == null)
            throw new NoSuchElementException();
    }

    private int findIndex(String name)
    {
        return nameIndex.getOrDefault(name, -1);
    }
}
