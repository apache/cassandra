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

package org.apache.cassandra.simulator.utils;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Spliterators.spliteratorUnknownSize;

/**
 * A simple intrusive double-linked list for maintaining a list of tasks,
 * useful for invalidating queued ordered tasks
 */
@SuppressWarnings("unchecked")
public class IntrusiveLinkedList<O extends IntrusiveLinkedListNode> extends IntrusiveLinkedListNode
{
    public IntrusiveLinkedList()
    {
        prev = next = this;
    }

    public void add(O add)
    {
        assert add.prev == null && add.next == null;
        IntrusiveLinkedListNode after = this;
        IntrusiveLinkedListNode before = prev;
        add.next = after;
        add.prev = before;
        before.next = add;
        after.prev = add;
    }

    public O poll()
    {
        if (isEmpty())
            return null;

        IntrusiveLinkedListNode next = this.next;
        next.remove();
        return (O) next;
    }

    public boolean isEmpty()
    {
        return next == this;
    }

    public Stream<O> stream()
    {
        Iterator<O> iterator = new Iterator<O>()
        {
            IntrusiveLinkedListNode next = IntrusiveLinkedList.this.next;

            @Override
            public boolean hasNext()
            {
                return next != IntrusiveLinkedList.this;
            }

            @Override
            public O next()
            {
                O result = (O)next;
                next = next.next;
                return result;
            }
        };

        return StreamSupport.stream(spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE), false);
    }
}

