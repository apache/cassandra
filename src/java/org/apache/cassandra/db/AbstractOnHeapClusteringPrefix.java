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
package org.apache.cassandra.db;

public abstract class AbstractOnHeapClusteringPrefix<V> implements ClusteringPrefix<V>
{
    protected final Kind kind;
    protected final V[] values;

    public AbstractOnHeapClusteringPrefix(Kind kind, V[] values)
    {
        this.kind = kind;
        this.values = values;
    }

    public Kind kind()
    {
        return kind;
    }

    public ClusteringPrefix<V> clustering()
    {
        return this;
    }

    public int size()
    {
        return values.length;
    }

    public V get(int i)
    {
        return values[i];
    }

    public V[] getRawValues()
    {
        return values;
    }

    @Override
    public int hashCode()
    {
        return ClusteringPrefix.hashCode(this);
    }

    @Override
    public boolean equals(Object o)
    {
        return ClusteringPrefix.equals(this, o);
    }
}
