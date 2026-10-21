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

package org.apache.cassandra.service.accord.journal;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.service.accord.JournalKey;

public interface Merger
{
    abstract class SimpleMerger<A, V> implements Merger
    {
        protected A accumulated;

        public SimpleMerger(A initial) {this.accumulated = initial; }
        public void update(V newValue) { accumulated = merge(accumulated, newValue); }
        public A get() { return accumulated; }

        protected abstract A merge(A oldValue, V newValue);
    }

    class KeepFirst<V> extends SimpleMerger<V, V>
    {
        final V ifNone;
        boolean hasRead;
        public KeepFirst(V ifNone)
        {
            super(ifNone);
            this.ifNone = ifNone;
        }

        @Override
        public void reset(JournalKey key)
        {
            hasRead = false;
            accumulated = ifNone;
        }

        @Override
        protected V merge(V oldValue, V newValue)
        {
            if (hasRead)
                return oldValue;
            hasRead = true;
            return newValue;
        }

        @Override
        public String toString()
        {
            return "KeepFirst{" + accumulated + '}';
        }
    }

    class KeepList<V> extends SimpleMerger<List<V>, V>
    {
        public KeepList(List<V> initial)
        {
            super(initial);
        }

        public KeepList()
        {
            super(new ArrayList<>());
        }

        @Override
        protected List<V> merge(List<V> oldValue, V newValue)
        {
            oldValue.add(newValue);
            return oldValue;
        }

        @Override
        public void reset(JournalKey key)
        {
            accumulated.clear();
        }
    }

    void reset(JournalKey key);
}
