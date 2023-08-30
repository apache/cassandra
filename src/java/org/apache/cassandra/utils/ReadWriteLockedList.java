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

package org.apache.cassandra.utils;

import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockedList<T> extends AbstractList<T>
{
    private final List<T> list;
    private final Lock readLock;
    private final Lock writeLock;

    public ReadWriteLockedList(List<T> list)
    {
        this.list = list;
        ReadWriteLock rwLock = new ReentrantReadWriteLock();
        readLock = rwLock.readLock();
        writeLock = rwLock.writeLock();
    }

    @Override
    public T set(int index, T element)
    {
        writeLock.lock();
        try
        {
            return list.set(index, element);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @Override
    public boolean add(T item)
    {
        writeLock.lock();
        try
        {
            return list.add(item);
        }
        finally
        {
            writeLock.unlock();
        }
    }

    @Override
    public T get(int index)
    {
        readLock.lock();
        try
        {
            return list.get(index);
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public int size()
    {
        readLock.lock();
        try
        {
            return list.size();
        }
        finally
        {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty()
    {
        readLock.lock();
        try
        {
            return list.isEmpty();
        }
        finally
        {
            readLock.unlock();
        }
    }

    public static <T> ReadWriteLockedList<T> wrap(List<T> list)
    {
        return new ReadWriteLockedList<>(list);
    }
}