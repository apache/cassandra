/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.btree;

import java.util.function.BiFunction;

import com.google.common.base.Function;
/**
 * An interface defining a function to be applied to both the object we are replacing in a BTree and
 * the object that is intended to replace it, returning the object to actually replace it.
 */
public interface UpdateFunction<K, V> extends Function<K, V>
{
    /**
     * @param replacing the value in the original tree we have matched
     * @param update the value in the updating collection that matched
     * @return the value to insert into the new tree
     */
    V apply(V replacing, K update);

    /**
     * @return true if we should fail the update
     */
    boolean abortEarly();

    /**
     * @param heapSize extra heap space allocated (over previous tree)
     */
    void allocated(long heapSize);

    public static final class Simple<V> implements UpdateFunction<V, V>
    {
        private final BiFunction<V, V, V> wrapped;
        public Simple(BiFunction<V, V, V> wrapped)
        {
            this.wrapped = wrapped;
        }

        public V apply(V v) { return v; }
        public V apply(V replacing, V update) { return wrapped.apply(replacing, update); }
        public boolean abortEarly() { return false; }
        public void allocated(long heapSize) { }

        public static <V> Simple<V> of(BiFunction<V, V, V> f)
        {
            return new Simple<>(f);
        }
    }

    static final Simple<Object> noOp = Simple.of((a, b) -> a);

    public static <K> UpdateFunction<K, K> noOp()
    {
        return (UpdateFunction<K, K>) noOp;
    }
}
