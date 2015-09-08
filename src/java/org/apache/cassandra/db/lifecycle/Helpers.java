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
package org.apache.cassandra.db.lifecycle;

import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.*;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Predicates.*;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import static org.apache.cassandra.utils.Throwables.merge;

class Helpers
{
    /**
     * update the contents of a set with the provided sets, ensuring that the items to remove are
     * really present, and that the items to add are not (unless we're also removing them)
     * @return a new set with the contents of the provided one modified
     */
    static <T> Set<T> replace(Set<T> original, Set<T> remove, Iterable<T> add)
    {
        return ImmutableSet.copyOf(replace(identityMap(original), remove, add).keySet());
    }

    /**
     * update the contents of an "identity map" with the provided sets, ensuring that the items to remove are
     * really present, and that the items to add are not (unless we're also removing them)
     * @return a new identity map with the contents of the provided one modified
     */
    static <T> Map<T, T> replace(Map<T, T> original, Set<T> remove, Iterable<T> add)
    {
        // ensure the ones being removed are the exact same ones present
        for (T reader : remove)
            assert original.get(reader) == reader;

        // ensure we don't already contain any we're adding, that we aren't also removing
        assert !any(add, and(not(in(remove)), in(original.keySet()))) : String.format("original:%s remove:%s add:%s", original.keySet(), remove, add);

        Map<T, T> result =
            identityMap(concat(add, filter(original.keySet(), not(in(remove)))));

        assert result.size() == original.size() - remove.size() + Iterables.size(add) :
        String.format("Expecting new size of %d, got %d while replacing %s by %s in %s",
                      original.size() - remove.size() + Iterables.size(add), result.size(), remove, add, original.keySet());
        return result;
    }

    /**
     * A convenience method for encapsulating this action over multiple SSTableReader with exception-safety
     * @return accumulate if not null (with any thrown exception attached), or any thrown exception otherwise
     */
    static void setupOnline(Iterable<SSTableReader> readers)
    {
        for (SSTableReader reader : readers)
            reader.setupOnline();
    }

    /**
     * A convenience method for encapsulating this action over multiple SSTableReader with exception-safety
     * @return accumulate if not null (with any thrown exception attached), or any thrown exception otherwise
     */
    static Throwable setReplaced(Iterable<SSTableReader> readers, Throwable accumulate)
    {
        for (SSTableReader reader : readers)
        {
            try
            {
                reader.setReplaced();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    /**
     * assert that none of these readers have been replaced
     */
    static void checkNotReplaced(Iterable<SSTableReader> readers)
    {
        for (SSTableReader reader : readers)
            assert !reader.isReplaced();
    }

    static Throwable markObsolete(List<LogTransaction.Obsoletion> obsoletions, Throwable accumulate)
    {
        if (obsoletions == null || obsoletions.isEmpty())
            return accumulate;

        for (LogTransaction.Obsoletion obsoletion : obsoletions)
        {
            try
            {
                obsoletion.reader.markObsolete(obsoletion.tidier);
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    static Throwable prepareForObsoletion(Iterable<SSTableReader> readers, LogTransaction txnLogs, List<LogTransaction.Obsoletion> obsoletions, Throwable accumulate)
    {
        for (SSTableReader reader : readers)
        {
            try
            {
                obsoletions.add(new LogTransaction.Obsoletion(reader, txnLogs.obsoleted(reader)));
            }
            catch (Throwable t)
            {
                accumulate = Throwables.merge(accumulate, t);
            }
        }
        return accumulate;
    }

    static Throwable abortObsoletion(List<LogTransaction.Obsoletion> obsoletions, Throwable accumulate)
    {
        if (obsoletions == null || obsoletions.isEmpty())
            return accumulate;

        for (LogTransaction.Obsoletion obsoletion : obsoletions)
        {
            try
            {
                obsoletion.tidier.abort();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }
        }
        return accumulate;
    }

    /**
     * @return the identity function, as a Map, with domain of the provided values
     */
    static <T> Map<T, T> identityMap(Iterable<T> values)
    {
        ImmutableMap.Builder<T, T> builder = ImmutableMap.<T, T>builder();
        for (T t : values)
            builder.put(t, t);
        return builder.build();
    }

    /**
     * @return an Iterable of the union if the sets, with duplicates being represented by their first encountered instance
     * (as defined by the order of set provision)
     */
    static <T> Iterable<T> concatUniq(Set<T>... sets)
    {
        List<Predicate<T>> notIn = new ArrayList<>(sets.length);
        for (Set<T> set : sets)
            notIn.add(not(in(set)));
        List<Iterable<T>> results = new ArrayList<>(sets.length);
        for (int i = 0 ; i < sets.length ; i++)
            results.add(filter(sets[i], and(notIn.subList(0, i))));
        return concat(results);
    }

    /**
     * @return a Predicate yielding true for an item present in NONE of the provided sets
     */
    static <T> Predicate<T> notIn(Set<T>... sets)
    {
        return not(orIn(sets));
    }

    /**
     * @return a Predicate yielding true for an item present in ANY of the provided sets
     */
    static <T> Predicate<T> orIn(Collection<T>... sets)
    {
        Predicate<T>[] orIn = new Predicate[sets.length];
        for (int i = 0 ; i < orIn.length ; i++)
            orIn[i] = in(sets[i]);
        return or(orIn);
    }

    /**
     * filter out (i.e. remove) matching elements
     * @return filter, filtered to only those elements that *are not* present in *any* of the provided sets (are present in none)
     */
    static <T> Iterable<T> filterOut(Iterable<T> filter, Set<T>... inNone)
    {
        return filter(filter, notIn(inNone));
    }

    /**
     * filter in (i.e. retain)
     *
     * @return filter, filtered to only those elements that *are* present in *any* of the provided sets
     */
    static <T> Iterable<T> filterIn(Iterable<T> filter, Set<T>... inAny)
    {
        return filter(filter, orIn(inAny));
    }

    static Set<SSTableReader> emptySet()
    {
        return Collections.emptySet();
    }

    static <T> T select(T t, Collection<T> col)
    {
        if (col instanceof Set && !col.contains(t))
            return null;
        return getFirst(filter(col, equalTo(t)), null);
    }

    static <T> T selectFirst(T t, Collection<T> ... sets)
    {
        for (Collection<T> set : sets)
        {
            T select = select(t, set);
            if (select != null)
                return select;
        }
        return null;
    }

    static <T> Predicate<T> idIn(Set<T> set)
    {
        return idIn(identityMap(set));
    }

    static <T> Predicate<T> idIn(final Map<T, T> identityMap)
    {
        return new Predicate<T>()
        {
            public boolean apply(T t)
            {
                return identityMap.get(t) == t;
            }
        };
    }

}
