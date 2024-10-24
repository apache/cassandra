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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ConcurrentLinkedStack<T>
{
    static final class Node<T> extends IntrusiveStack<Node<T>>
    {
        final T value;
        Node(T value)
        {
            this.value = value;
        }
    }

    private volatile Node<T> head;
    private static final AtomicReferenceFieldUpdater<ConcurrentLinkedStack, Node> headUpdater = AtomicReferenceFieldUpdater.newUpdater(ConcurrentLinkedStack.class, Node.class, "head");

    public void push(T value)
    {
        IntrusiveStack.push(headUpdater, this, (Node)new Node<>(value));
    }

    public boolean isEmpty()
    {
        return head == null;
    }

    public void drain(Consumer<T> forEach, boolean reverse)
    {
        if (isEmpty())
            return;

        Node<T> head = headUpdater.getAndSet(this, null);
        if (reverse) head = IntrusiveStack.reverse(head);
        IntrusiveStack.forEach(head, n -> n.value, forEach);
    }

    public <P> void drain(BiConsumer<P, T> forEach, P param, boolean reverse)
    {
        if (isEmpty())
            return;

        Node<T> head = headUpdater.getAndSet(this, null);
        if (reverse) head = IntrusiveStack.reverse(head);
        IntrusiveStack.forEach(head, n -> n.value, forEach, param);
    }
}
