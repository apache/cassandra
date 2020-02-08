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

import java.util.Date;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

public class ResultSet implements Iterator<Row>
{
    public static final ResultSet EMPTY = new ResultSet(new String[0], null);

    private final String[] names;
    private final Object[][] results;
    private final Predicate<Row> filter;
    private final Row row;
    private int offset = -1;

    public ResultSet(String[] names, Object[][] results)
    {
        this.names = names;
        this.results = results;
        this.row = new Row(names);
        this.filter = ignore -> true;
    }

    private ResultSet(String[] names, Object[][] results, Predicate<Row> filter, int offset)
    {
        this.names = names;
        this.results = results;
        this.filter = filter;
        this.offset = offset;
        this.row = new Row(names);
    }

    public String[] getNames()
    {
        return names;
    }

    public boolean isEmpty()
    {
        return results.length == 0;
    }

    public int size()
    {
        return results.length;
    }

    public ResultSet filter(Predicate<Row> fn)
    {
        return new ResultSet(names, results, filter.and(fn), offset);
    }

    public Object[][] toObjectArrays()
    {
        return results;
    }

    @Override
    public boolean hasNext()
    {
        if (results == null)
            return false;
        while ((offset += 1) < results.length)
        {
            row.setResults(results[offset]);
            if (filter.test(row))
            {
                return true;
            }
        }
        row.setResults(null);
        return false;
    }

    @Override
    public Row next()
    {
        if (offset < 0 || offset >= results.length)
            throw new NoSuchElementException();
        return row;
    }

    public <T> T get(String name)
    {
        return next().get(name);
    }

    public String getString(String name)
    {
        return next().getString(name);
    }

    public UUID getUUID(String name)
    {
        return next().getUUID(name);
    }

    public Date getTimestamp(String name)
    {
        return next().getTimestamp(name);
    }

    public <T> Set<T> getSet(String name)
    {
        return next().getSet(name);
    }
}
