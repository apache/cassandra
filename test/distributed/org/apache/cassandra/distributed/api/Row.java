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
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

public class Row
{
    private final String[] names;
    @Nullable private Object[] results; // mutable to avoid allocations in loops

    public Row(String[] names)
    {
        this.names = names;
    }

    void setResults(@Nullable Object[] results)
    {
        this.results = results;
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
               "names=" + Arrays.toString(names) +
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
        for (int i = 0; i < names.length; i++)
            if (names[i].equals(name))
                return i;
        return -1;
    }
}
