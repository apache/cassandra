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

import java.util.Objects;

import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.utils.ObjectSizes;

public class StoredLong extends AbstractStoredField
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new StoredLong(AccordState.Kind.FULL));

    protected long value;

    public StoredLong(AccordState.Kind kind)
    {
        super(kind);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoredLong that = (StoredLong) o;
        return value == that.value;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value);
    }

    @Override
    public String valueString()
    {
        return Long.toString(value);
    }

    public long estimatedSizeOnHeap()
    {
        return EMPTY_SIZE;
    }

    public void unload()
    {
        preUnload();
        value = 0;
    }

    public void load(long value)
    {
        preLoad();
        this.value = value;
    }

    public void set(long value)
    {
        preChange();
        this.value = value;
    }

    public long get()
    {
        preGet();
        return value;
    }
}
