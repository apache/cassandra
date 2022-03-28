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
package org.apache.cassandra.schema;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.Iterables;

public class Diff<T extends Iterable, S>
{
    public final T created;
    public final T dropped;
    public final ImmutableCollection<Altered<S>> altered;

    Diff(T created, T dropped, ImmutableCollection<Altered<S>> altered)
    {
        this.created = created;
        this.dropped = dropped;
        this.altered = altered;
    }

    boolean isEmpty()
    {
        return Iterables.isEmpty(created) && Iterables.isEmpty(dropped) && Iterables.isEmpty(altered);
    }

    Iterable<Altered<S>> altered(Difference kind)
    {
        return Iterables.filter(altered, a -> a.kind == kind);
    }

    public static final class Altered<T>
    {
        public final T before;
        public final T after;
        public final Difference kind;

        Altered(T before, T after, Difference kind)
        {
            this.before = before;
            this.after = after;
            this.kind = kind;
        }

        public String toString()
        {
            return String.format("%s -> %s (%s)", before, after, kind);
        }
    }

    @Override
    public String toString()
    {
        return "Diff{" +
               "created=" + created +
               ", dropped=" + dropped +
               ", altered=" + altered +
               '}';
    }

}
