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

package org.apache.cassandra.service.accord.serializers;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

import static accord.utils.ArrayBuffers.cachedAny;

public abstract class AbstractSortedCollector<T, C> extends AbstractList<T>
{
    private static final int BTREE_THRESHOLD = 16;

    Object buffer;
    int count = 0;

    abstract Comparator<Object> comparator();
    abstract C empty();
    abstract C of(T one);
    abstract C copy(Object[] array, int count);
    abstract C copyBtree(Object[] btree, int count);

    public AbstractSortedCollector()
    {
    }

    public boolean add(T add)
    {
        return add == collect(add);
    }

    protected T collect(T add)
    {
        if (count == 0)
        {
            buffer = add;
            count = 1;
            return add;
        }
        if (count == 1)
        {
            if (add.equals(buffer))
                return (T)buffer;
            Object[] newBuffer = cachedAny().get(8);
            boolean addIsLower = comparator().compare(add, buffer) < 0;
            newBuffer[0] = addIsLower ? add : buffer;
            newBuffer[1] = addIsLower ? buffer : add;
            buffer = newBuffer;
            count = 2;
            return add;
        }
        Object[] buffer = (Object[]) this.buffer;
        if (count < BTREE_THRESHOLD)
        {
            int i = Arrays.binarySearch(buffer, 0, count, add, comparator());
            if (i >= 0)
                return (T) buffer[i];
            i = -1 - i;
            if (count == buffer.length)
                this.buffer = buffer = cachedAny().resize(buffer, count, count + 1);
            System.arraycopy(buffer, i, buffer, i + 1, count - i);
            buffer[i] = add;
            if (++count == BTREE_THRESHOLD)
            {
                Object[] btree = BTree.build(BulkIterator.of(buffer), count, UpdateFunction.noOp());
                cachedAny().forceDiscard(buffer, count);
                this.buffer = btree;
            }
            return add;
        }
        Object existing = BTree.find(buffer, comparator(), add);
        if (existing != null)
            return (T)existing;
        this.buffer = BTree.update(buffer, BTree.singleton(add), comparator());
        ++count;
        return add;
    }

    public void clear()
    {
        if (count > 1)
            cachedAny().forceDiscard((Object[])buffer, count);
        buffer = null;
        count = 0;
    }

    public C build()
    {
        if (count == 0)
        {
            return empty();
        }
        else if (count == 1)
        {
            return of((T)buffer);
        }
        else if (count < BTREE_THRESHOLD)
        {
            C result = copy((Object[])buffer, count);
            cachedAny().forceDiscard((Object[])buffer, count);
            return result;
        }
        else
        {
            return copyBtree((Object[])buffer, count);
        }
    }

    @Override
    public T get(int index)
    {
        if (index < 0 || index >= count) throw new IndexOutOfBoundsException();
        if (count == 1) return (T) buffer;
        if (count < BTREE_THRESHOLD)
            return (T) ((Object[])buffer)[index];
        return BTree.findByIndex((Object[])buffer, index);
    }

    @Override
    public int size()
    {
        return count;
    }
}
