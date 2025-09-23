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
package org.apache.cassandra.journal;

public interface KeyStats<K>
{
    KeyStats<?> NOOP = (KeyStats<Object>) o -> true;

    static <K> KeyStats<K> noop()
    {
        //noinspection unchecked
        return (KeyStats<K>) NOOP;
    }

    boolean mayContain(K k);

    interface Active<K> extends KeyStats<K>
    {
        Active<Object> NOOP = new Active<>()
        {
            @Override
            public void update(Object key)
            {
                // no-op
            }

            @Override
            public boolean mayContain(Object key)
            {
                return true;
            }

            @Override
            public void persist(Descriptor descriptor)
            {
                // no-op
            }
        };

        void update(K key);
        void persist(Descriptor descriptor);
    }

    interface Static<K> extends KeyStats<K>
    {
        Static<Object> NOOP = key -> true;
    }

    interface Factory<K>
    {
        Factory<?> NOOP = new Factory<>()
        {
            @Override
            public Active<Object> create()
            {
                return Active.NOOP;
            }

            @Override
            public Static<Object> load(Descriptor descriptor)
            {
                return Static.NOOP;
            }

            @Override
            public Active<Object> rebuild(Descriptor descriptor, KeySupport<Object> keySupport, int fsyncedLimit)
            {
                return Active.NOOP;
            }
        };

        static <K> Factory<K> noop()
        {
            //noinspection unchecked
            return (Factory<K>) NOOP;
        }

        Active<K> create();
        Static<K> load(Descriptor descriptor);
        Active<K> rebuild(Descriptor descriptor, KeySupport<K> keySupport, int fsyncedLimit);
    }
}
