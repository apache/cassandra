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
import java.util.function.ToLongFunction;

import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.utils.ObjectSizes;

public class StoredValue<T> extends AbstractStoredField
{
    public static final long EMPTY_SIZE = ObjectSizes.measure(new StoredValue<>(AccordState.Kind.FULL));
    protected T value;

    public StoredValue(AccordState.Kind kind)
    {
        super(kind);
    }

    @Override
    public boolean equals(Object o)
    {
        this.preGet();
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoredValue<?> that = (StoredValue<?>) o;
        that.preGet();
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        preGet();
        return Objects.hash(value);
    }

    @Override
    public String valueString()
    {
        return Objects.toString(value);
    }

    public long estimatedSizeOnHeap(ToLongFunction<T> measure)
    {
        if (!hasValue() || value == null)
            return EMPTY_SIZE;

        return EMPTY_SIZE + measure.applyAsLong(value);
    }

    public void unload()
    {
        preUnload();
        value = null;
    }

    public void load(T value)
    {
        preLoad();
        this.value = value;
    }

    public void set(T value)
    {
        preChange();
        this.value = value;
    }

    public T get()
    {
        preGet();
        return value;
    }

    public static class HistoryPreserving<T> extends StoredValue<T>
    {
        T previous;

        public HistoryPreserving(AccordState.Kind kind)
        {
            super(kind);
        }

        public T previous()
        {
            return previous;
        }

        @Override
        public void unload()
        {
            super.unload();
            previous = null;
        }

        @Override
        public void load(T value)
        {
            super.load(value);
            previous = value;
        }

        @Override
        public void clearModifiedFlag()
        {
            super.clearModifiedFlag();
            previous = value;
        }
    }
}
