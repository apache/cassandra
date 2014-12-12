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
package org.apache.cassandra.stress.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.cassandra.stress.generate.FasterRandom;

// simple thread-unsafe skiplist that permits indexing/removal by position, insertion at the end
// (though easily extended to insertion at any position, not necessary here)
// we use it for sampling items by position for visiting writes in the pool of pending writes
public class DynamicList<E>
{

    // represents a value and an index simultaneously; each node maintains a list
    // of next pointers for each height in the skip-list this node participates in
    // (a contiguous range from [0..height))
    public static class Node<E>
    {
        // stores the size of each descendant
        private final int[] size;
        // TODO: alternate links to save space
        private final Node<E>[] links;
        private E value;

        private Node(int height, E value)
        {
            this.value = value;
            links = new Node[height * 2];
            size = new int[height];
            Arrays.fill(size, 1);
        }

        private int height()
        {
            return size.length;
        }

        private Node<E> next(int i)
        {
            return links[i * 2];
        }

        private Node<E> prev(int i)
        {
            return links[1 + i * 2];
        }

        private void setNext(int i, Node<E> next)
        {
            links[i * 2] = next;
        }

        private void setPrev(int i, Node<E> prev)
        {
            links[1 + i * 2] = prev;
        }

        private Node parent(int parentHeight)
        {
            Node prev = this;
            while (true)
            {
                int height = prev.height();
                if (parentHeight < height)
                    return prev;
                prev = prev.prev(height - 1);
            }
        }
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final int maxHeight;
    private final Node<E> head;
    private int size;

    public DynamicList(int maxExpectedSize)
    {
        this.maxHeight = 3 + Math.max(0, (int) Math.ceil(Math.log(maxExpectedSize) / Math.log(2)));
        head = new Node<>(maxHeight, null);
    }

    private int randomLevel()
    {
        return 1 + Integer.bitCount(ThreadLocalRandom.current().nextInt() & ((1 << (maxHeight - 1)) - 1));
    }

    public Node<E> append(E value)
    {
        return append(value, Integer.MAX_VALUE);
    }

    // add the value to the end of the list, and return the associated Node that permits efficient removal
    // regardless of its future position in the list from other modifications
    public Node<E> append(E value, int maxSize)
    {
        Node<E> newTail = new Node<>(randomLevel(), value);

        lock.writeLock().lock();
        try
        {
            if (size >= maxSize)
                return null;
            size++;

            Node<E> tail = head;
            for (int i = maxHeight - 1 ; i >= newTail.height() ; i--)
            {
                Node<E> next;
                while ((next = tail.next(i)) != null)
                    tail = next;
                tail.size[i]++;
            }

            for (int i = newTail.height() - 1 ; i >= 0 ; i--)
            {
                Node<E> next;
                while ((next = tail.next(i)) != null)
                    tail = next;
                tail.setNext(i, newTail);
                newTail.setPrev(i, tail);
            }

            return newTail;
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
        assert node.value != null;
        node.value = null;
        try
        {
            size--;

            // go up through each level in the skip list, unlinking this node; this entails
            // simply linking each neighbour to each other, and appending the size of the
            // current level owned by this node's index to the preceding neighbour (since
            // ownership is defined as any node that you must visit through the index,
            // removal of ourselves from a level means the preceding index entry is the
            // entry point to all of the removed node's descendants)
            for (int i = 0 ; i < node.height() ; i++)
            {
                Node<E> prev = node.prev(i);
                Node<E> next = node.next(i);
                assert prev != null;
                prev.setNext(i, next);
                if (next != null)
                    next.setPrev(i, prev);
                prev.size[i] += node.size[i] - 1;
            }

            // then go up the levels, removing 1 from the size at each height above ours
            for (int i = node.height() ; i < maxHeight ; i++)
            {
                // if we're at our height limit, we backtrack at our top level until we
                // hit a neighbour with a greater height
                while (i == node.height())
                    node = node.prev(i - 1);
                node.size[i]--;
            }
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
            if (index >= size)
                return null;

            index++;
            int c = 0;
            Node<E> finger = head;
            for (int i = maxHeight - 1 ; i >= 0 ; i--)
            {
                while (c + finger.size[i] <= index)
                {
                    c += finger.size[i];
                    finger = finger.next(i);
                }
            }

            assert c == index;
            return finger.value;
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    // some quick and dirty tests to confirm the skiplist works as intended
    // don't create a separate unit test - tools tree doesn't currently warrant them

    private boolean isWellFormed()
    {
        for (int i = 0 ; i < maxHeight ; i++)
        {
            int c = 0;
            for (Node node = head ; node != null ; node = node.next(i))
            {
                if (node.prev(i) != null && node.prev(i).next(i) != node)
                    return false;
                if (node.next(i) != null && node.next(i).prev(i) != node)
                    return false;
                c += node.size[i];
                if (i + 1 < maxHeight && node.parent(i + 1).next(i + 1) == node.next(i))
                {
                    if (node.parent(i + 1).size[i + 1] != c)
                        return false;
                    c = 0;
                }
            }
            if (i == maxHeight - 1 && c != size + 1)
                return false;
        }
        return true;
    }

    public static void main(String[] args)
    {
        DynamicList<Integer> list = new DynamicList<>(20);
        TreeSet<Integer> canon = new TreeSet<>();
        HashMap<Integer, Node> nodes = new HashMap<>();
        int c = 0;
        for (int i = 0 ; i < 100000 ; i++)
        {
            nodes.put(c, list.append(c));
            canon.add(c);
            c++;
        }
        FasterRandom rand = new FasterRandom();
        assert list.isWellFormed();
        for (int loop = 0 ; loop < 100 ; loop++)
        {
            System.out.println(loop);
            for (int i = 0 ; i < 100000 ; i++)
            {
                int index = rand.nextInt(100000);
                Integer seed = list.get(index);
//                assert canon.headSet(seed, false).size() == index;
                list.remove(nodes.remove(seed));
                canon.remove(seed);
                nodes.put(c, list.append(c));
                canon.add(c);
                c++;
            }
            assert list.isWellFormed();
        }
    }

}
