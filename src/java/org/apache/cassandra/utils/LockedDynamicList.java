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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// simple thread-unsafe skiplist that permits indexing/removal by position, insertion at the end
// (though easily extended to insertion at any position, not necessary here)
// we use it for sampling items by position for visiting writes in the pool of pending writes
public class LockedDynamicList<E> extends DynamicList<E>
{

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LockedDynamicList(int maxExpectedSize)
    {
        super(maxExpectedSize);
    }

    // add the value to the end of the list, and return the associated Node that permits efficient removal
    // regardless of its future position in the list from other modifications
    public Node<E> append(E value, int maxSize)
    {
        lock.writeLock().lock();
        try
        {
            return super.append(value, maxSize);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    // remove the provided node and its associated value from the list
    public void remove(Node<E> node)
    {
        lock.writeLock().lock();
        try
        {
            super.remove(node);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    // retrieve the item at the provided index, or return null if the index is past the end of the list
    public E get(int index)
    {
        lock.readLock().lock();
        try
        {
            return super.get(index);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public int size()
    {
        lock.readLock().lock();
        try
        {
            return super.size();
        }
        finally
        {
            lock.readLock().unlock();
        }
    }
}
