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
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.utils.TimeUUID;

/**
 * Data representing a single row in a query result.
 * <p>
 * This class is mutable from the parent {@link SimpleQueryResult} and can have the row it points to changed between calls
 * to {@link SimpleQueryResult#hasNext()}, for this reason it is unsafe to hold reference to this class after that call;
 * to get around this, a call to {@link #copy()} will return a new object pointing to the same row.
 */
public class Row
{
    private static final int NOT_FOUND = -1;

    private final String[] names;
    private final Map<String, Integer> nameIndex;
    private Object[] results; // mutable to avoid allocations in loops

    public Row(String[] names)
    {
        Objects.requireNonNull(names, "names");
        this.names = names;
        this.nameIndex = new HashMap<>(names.length);
        for (int i = 0; i < names.length; i++)
        {
            // if duplicate names, always index by the first one seen
            nameIndex.putIfAbsent(names[i], i);
        }
    }

    private Row(String[] names, Map<String, Integer> nameIndex)
    {
        this.names = names;
        this.nameIndex = nameIndex;
    }

    public void setResults(Object[] results)
    {
        this.results = results;
    }

    /**
     * Creates a copy of the current row; can be used past calls to {@link SimpleQueryResult#hasNext()}.
     */
    public Row copy()
    {
        Row copy = new Row(names, nameIndex);
        copy.setResults(results);
        return copy;
    }

    public <T> T get(int index)
    {
        checkAccess();
        if (index < 0 || index >= results.length)
            throw new NoSuchElementException("by index: " + index);
        return (T) results[index];
    }

    public <T> T get(String name)
    {
        checkAccess();
        int idx = findIndex(name);
        if (idx == NOT_FOUND)
            throw new NoSuchElementException("by name: " + name);
        return (T) results[idx];
    }

    public Short getShort(int index)
    {
        return get(index);
    }

    public Short getShort(String name)
    {
        return get(name);
    }

    public Integer getInteger(int index)
    {
        return get(index);
    }

    public Integer getInteger(String name)
    {
        return get(name);
    }

    public Long getLong(int index)
    {
        return get(index);
    }

    public Long getLong(String name)
    {
        return get(name);
    }

    public Float getFloat(int index)
    {
        return get(index);
    }

    public Float getFloat(String name)
    {
        return get(name);
    }

    public Double getDouble(int index)
    {
        return get(index);
    }

    public Double getDouble(String name)
    {
        return get(name);
    }

    public String getString(int index)
    {
        return get(index);
    }

    public String getString(String name)
    {
        return get(name);
    }

    public UUID getUUID(int index)
    {
        Object uuid = get(index);
        if (uuid instanceof TimeUUID)
            return ((TimeUUID) uuid).asUUID();
        return (UUID) uuid;
    }

    public UUID getUUID(String name)
    {
        Object uuid = get(name);
        if (uuid instanceof TimeUUID)
            return ((TimeUUID) uuid).asUUID();
        return (UUID) uuid;
    }

    public Date getTimestamp(int index)
    {
        return get(index);
    }

    public Date getTimestamp(String name)
    {
        return get(name);
    }

    public <T> Set<T> getSet(int index)
    {
        return get(index);
    }

    public <T> Set<T> getSet(String name)
    {
        return get(name);
    }

    /**
     * Get the row as a array.
     */
    public Object[] toObjectArray()
    {
        return results;
    }

    public String toString()
    {
        return "Row{" +
               "names=" + Arrays.toString(names) +
               ", results=" + (results == null ? "[]" : Arrays.toString(results)) +
               '}';
    }

    private void checkAccess()
    {
        if (results == null)
            throw new NoSuchElementException();
    }

    private int findIndex(String name)
    {
        return nameIndex.getOrDefault(name, NOT_FOUND);
    }
}
