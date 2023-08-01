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

package org.apache.cassandra.utils.concurrent;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.utils.LongAccumulator;

/**
 * An efficient stack/list that is expected to be ordinarily either empty or close to, and for which
 * we need concurrent insertions and do not need to support removal - i.e. the list is semi immutable.
 *
 * This is an intrusive stack, and for simplicity we treat all
 *
 * @param <T>
 */
public class IntrusiveStack<T extends IntrusiveStack<T>> implements Iterable<T>
{
    static class Itr<T extends IntrusiveStack<T>> implements Iterator<T>
    {
        private T next;

        Itr(T next)
        {
            this.next = next;
        }

        @Override
        public boolean hasNext()
        {
            return next != null;
        }

        @Override
        public T next()
        {
            T result = next;
            next = result.next;
            return result;
        }
    }

    T next;

    @Inline
    protected static <O, T extends IntrusiveStack<T>> T push(AtomicReferenceFieldUpdater<? super O, T> headUpdater, O owner, T prepend)
    {
        return push(headUpdater, owner, prepend, (prev, next) -> {
            next.next = prev;
            return next;
        });
    }

    protected static <O, T extends IntrusiveStack<T>> T push(AtomicReferenceFieldUpdater<O, T> headUpdater, O owner, T prepend, BiFunction<T, T, T> combine)
    {
        while (true)
        {
            T head = headUpdater.get(owner);
            if (headUpdater.compareAndSet(owner, head, combine.apply(head, prepend)))
                return head;
        }
    }

    protected interface Setter<O, T>
    {
        public boolean compareAndSet(O owner, T expect, T update);
    }

    @Inline
    protected static <O, T extends IntrusiveStack<T>> T push(Function<O, T> getter, Setter<O, T> setter, O owner, T prepend)
    {
        return push(getter, setter, owner, prepend, (prev, next) -> {
            next.next = prev;
            return next;
        });
    }

    protected static <O, T extends IntrusiveStack<T>> T push(Function<O, T> getter, Setter<O, T> setter, O owner, T prepend, BiFunction<T, T, T> combine)
    {
        while (true)
        {
            T head = getter.apply(owner);
            if (setter.compareAndSet(owner, head, combine.apply(head, prepend)))
                return head;
        }
    }

    protected static <O, T extends IntrusiveStack<T>> void pushExclusive(AtomicReferenceFieldUpdater<O, T> headUpdater, O owner, T prepend, BiFunction<T, T, T> combine)
    {
        T head = headUpdater.get(owner);
        headUpdater.lazySet(owner, combine.apply(head, prepend));
    }

    protected static <T extends IntrusiveStack<T>, O> void pushExclusive(AtomicReferenceFieldUpdater<O, T> headUpdater, O owner, T prepend)
    {
        prepend.next = headUpdater.get(owner);
        headUpdater.lazySet(owner, prepend);
    }

    protected static <T extends IntrusiveStack<T>> T pushExclusive(T head, T prepend)
    {
        prepend.next = head;
        return prepend;
    }

    protected static <T extends IntrusiveStack<T>, O> Iterable<T> iterable(AtomicReferenceFieldUpdater<O, T> headUpdater, O owner)
    {
        return iterable(headUpdater.get(owner));
    }

    protected static <T extends IntrusiveStack<T>> Iterable<T> iterable(T list)
    {
        return list == null ? () -> iterator(null) : list;
    }

    protected static <T extends IntrusiveStack<T>> Iterator<T> iterator(T list)
    {
        return new Itr<>(list);
    }

    protected static int size(IntrusiveStack<?> list)
    {
        int size = 0;
        while (list != null)
        {
            ++size;
            list = list.next;
        }
        return size;
    }

    protected static <T extends IntrusiveStack<T>> long accumulate(T list, LongAccumulator<T> accumulator, long initialValue)
    {
        long value = initialValue;
        while (list != null)
        {
            value = accumulator.apply(list, initialValue);
            list = list.next;
        }
        return value;
    }

    // requires exclusive ownership (incl. with readers)
    protected T reverse()
    {
        return reverse((T) this);
    }

    // requires exclusive ownership (incl. with readers)
    protected static <T extends IntrusiveStack<T>> T reverse(T list)
    {
        T prev = null;
        T cur = list;
        while (cur != null)
        {
            T next = cur.next;
            cur.next = prev;
            prev = cur;
            cur = next;
        }
        return prev;
    }

    @Override
    public void forEach(Consumer<? super T> forEach)
    {
        forEach((T)this, forEach);
    }

    protected static <T extends IntrusiveStack<T>> void forEach(T list, Consumer<? super T> forEach)
    {
        while (list != null)
        {
            forEach.accept(list);
            list = list.next;
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return new Itr<>((T) this);
    }
}
