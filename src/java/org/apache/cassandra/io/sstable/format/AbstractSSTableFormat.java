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

package org.apache.cassandra.io.sstable.format;

import java.util.Map;
import java.util.Objects;

public abstract class AbstractSSTableFormat<R extends SSTableReader, W extends SSTableWriter> implements SSTableFormat<R, W>
{
    public final String name;
    protected final Map<String, String> options;

    protected AbstractSSTableFormat(String name, Map<String, String> options)
    {
        this.name = Objects.requireNonNull(name);
        this.options = options;
    }

    @Override
    public final String name()
    {
        return name;
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractSSTableFormat<?, ?> that = (AbstractSSTableFormat<?, ?>) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(name);
    }

    @Override
    public String toString()
    {
        return name + ":" + options;
    }
}
