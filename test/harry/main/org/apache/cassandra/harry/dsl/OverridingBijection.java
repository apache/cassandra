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

package org.apache.cassandra.harry.dsl;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.harry.gen.Bijections;

class OverridingBijection<T> implements Bijections.Bijection<T>
{
    protected final Bijections.Bijection<T> delegate;
    protected final Map<Long, T> descriptorToValue;
    protected final Map<T, Long> valueToDescriptor;

    public OverridingBijection(Bijections.Bijection<T> delegate)
    {
        this.delegate = delegate;
        descriptorToValue = new HashMap<>();
        valueToDescriptor = new HashMap<>();
    }

    public void override(long descriptor, T value)
    {
        T old = descriptorToValue.get(descriptor);
        if (old != null)
            throw new IllegalStateException(String.format("Can't override %d twice. Was already overriden to %s", descriptor, old));

        T orig = delegate.inflate(descriptor);
        if (!orig.equals(value))
        {
            descriptorToValue.put(descriptor, value);
            valueToDescriptor.put(value, descriptor);
        }
    }

    @Override
    public T inflate(long descriptor)
    {
        Object v = descriptorToValue.get(descriptor);
        if (v != null)
        {
            return (T) v;
        }
        return delegate.inflate(descriptor);
    }

    @Override
    public long deflate(T value)
    {
        Long descriptor = valueToDescriptor.get(value);
        if (descriptor != null)
            return descriptor;
        return delegate.deflate(value);
    }

    @Override
    public int byteSize()
    {
        return delegate.byteSize();
    }

    @Override
    public int compare(long l, long r)
    {
        return delegate.compare(l, r);
    }
}
