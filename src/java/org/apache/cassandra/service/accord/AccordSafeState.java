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

import accord.impl.SafeState;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.AccordCachingState.Status;

public interface AccordSafeState<K, V> extends SafeState<V>
{
    void set(V update);
    V original();
    void invalidate();
    boolean invalidated();
    void preExecute();

    AccordCachingState<K, V> global();

    default boolean hasUpdate()
    {
        return original() != current();
    }

    default void revert()
    {
        set(original());
    }

    default K key()
    {
        return global().key();
    }

    default Status globalStatus()
    {
        return global().status();
    }

    default AsyncChain<?> loading()
    {
        return global().loading();
    }

    default AsyncChain<?> saving()
    {
        return global().saving();
    }

    default Throwable failure()
    {
        return global().failure();
    }

    default void checkNotInvalidated()
    {
        if (invalidated())
            throw new IllegalStateException("Cannot access invalidated " + this);
    }
}
