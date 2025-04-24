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

package org.apache.cassandra.service.accord;

import javax.annotation.Nullable;

public abstract class ImmutableAccordSafeState<K, V> implements AccordSafeState<K, V>
{
    protected final K key;
    @Nullable
    protected V original;
    protected boolean invalidated;

    protected ImmutableAccordSafeState(K key)
    {
        this.key = key;
    }

    @Override
    public K key()
    {
        return key;
    }

    @Override
    public V original()
    {
        checkNotInvalidated();
        return original;
    }

    @Override
    public V current()
    {
        checkNotInvalidated();
        return original;
    }

    @Override
    public void invalidate()
    {
        invalidated = true;
    }

    @Override
    public boolean invalidated()
    {
        return invalidated;
    }

    @Override
    public void set(V update)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void revert()
    {
        checkNotInvalidated();
    }
}
