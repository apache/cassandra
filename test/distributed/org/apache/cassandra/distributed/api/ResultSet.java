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

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ResultSet implements Iterator<Object[]>
{
    private final String[] names;
    private final Object[][] results;
    private int offset = -1;

    public ResultSet(String[] names, Object[][] results)
    {
        this.names = names;
        this.results = results;
    }

    public String[] getNames()
    {
        return names;
    }

    public boolean isEmpty()
    {
        return results.length == 0;
    }

    @Override
    public boolean hasNext()
    {
        return (offset += 1) < results.length;
    }

    @Override
    public Object[] next()
    {
        if (offset < 0 || offset >= results.length)
            throw new NoSuchElementException();
        return results[offset];
    }

    public String getString(String name)
    {
        int idx = findIndex(name);
        if (idx == -1)
            return null;
        return (String) next()[idx];
    }

    private int findIndex(String name)
    {
        for (int i = 0; i < name.length(); i++)
            if (names[i].equals(name))
                return i;
        return -1;
    }
}
