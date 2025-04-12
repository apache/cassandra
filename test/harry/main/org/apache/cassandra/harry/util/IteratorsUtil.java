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

package org.apache.cassandra.harry.util;

import java.util.Iterator;

public class IteratorsUtil
{
    public static <T> Iterable<T> concat(Iterable<T>... iterables)
    {
        assert iterables != null && iterables.length > 0;
        if (iterables.length == 1)
            return iterables[0];

        return () -> {
            return new Iterator<T>()
            {
                int idx;
                Iterator<T> current;
                boolean hasNext;

                {
                    idx = 0;
                    prepareNext();
                }

                private void prepareNext()
                {
                    if (current != null && current.hasNext())
                    {
                        hasNext = true;
                        return;
                    }

                    while (idx < iterables.length)
                    {
                        current = iterables[idx].iterator();
                        idx++;
                        if (current.hasNext())
                        {
                            hasNext = true;
                            return;
                        }
                    }

                    hasNext = false;
                }

                public boolean hasNext()
                {
                    return hasNext;
                }

                public T next()
                {
                    T next = current.next();
                    prepareNext();
                    return next;
                }
            };
        };
    }
}
