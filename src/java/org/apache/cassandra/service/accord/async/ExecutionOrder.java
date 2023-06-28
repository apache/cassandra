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
package org.apache.cassandra.service.accord.async;

import java.util.ArrayDeque;

import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.Object2ObjectHashMap;

/**
 * Assists with correct ordering of {@link AsyncOperation} execution wrt each other,
 * preventing reordering of overlapping operations by {@link AsyncLoader}.
 */
public class ExecutionOrder
{
    private final Object2ObjectHashMap<Object, Object> queues = new Object2ObjectHashMap<>();

    /**
     * Register an operation as having a dependency on its keys and TxnIds
     * @return true if no other operation depends on the keys or TxnIds, false otherwise
     */
    boolean register(AsyncOperation<?> operation)
    {
        boolean canRun = true;
        for (RoutableKey key : operation.keys())
            canRun &= register(key, operation);
        TxnId primaryTxnId = operation.primaryTxnId();
        if (null != primaryTxnId)
            canRun &= register(primaryTxnId, operation);
        return canRun;
    }

    /**
     * Register an operation as having a dependency on a key or a TxnId
     * @return true if no other operation depends on the key/TxnId, false otherwise
     */
    private boolean register(Object keyOrTxnId, AsyncOperation<?> operation)
    {
        Object operationOrQueue = queues.get(keyOrTxnId);
        if (null == operationOrQueue)
        {
            queues.put(keyOrTxnId, operation);
            return true;
        }

        if (operationOrQueue instanceof AsyncOperation)
        {
            ArrayDeque<AsyncOperation<?>> queue = new ArrayDeque<>(4);
            queue.add((AsyncOperation<?>) operationOrQueue);
            queue.add(operation);
            queues.put(keyOrTxnId, queue);
        }
        else
        {
            @SuppressWarnings("unchecked")
            ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
            queue.add(operation);
        }
        return false;
    }

    /**
     * Unregister the operation as being a dependency for its keys and TxnIds
     */
    void unregister(AsyncOperation<?> operation)
    {
        for (RoutableKey key : operation.keys())
            unregister(key, operation);
        TxnId primaryTxnId = operation.primaryTxnId();
        if (null != primaryTxnId)
            unregister(primaryTxnId, operation);
    }

    /**
     * Unregister the operation as being a dependency for key or TxnId
     */
    private void unregister(Object keyOrTxnId, AsyncOperation<?> operation)
    {
        Object operationOrQueue = queues.get(keyOrTxnId);
        Invariants.nonNull(operationOrQueue);

        if (operationOrQueue instanceof AsyncOperation<?>)
        {
            Invariants.checkState(operationOrQueue == operation);
            queues.remove(keyOrTxnId);
        }
        else
        {
            @SuppressWarnings("unchecked")
            ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
            AsyncOperation<?> head = queue.poll();
            Invariants.checkState(head == operation);

            if (queue.isEmpty())
            {
                queues.remove(keyOrTxnId);
            }
            else
            {
                head = queue.peek();
                if (canRun(head))
                    head.onUnblocked();
            }
        }
    }

    boolean canRun(AsyncOperation<?> operation)
    {
        for (RoutableKey key : operation.keys())
            if (!canRun(key, operation))
                return false;

        TxnId primaryTxnId = operation.primaryTxnId();
        return primaryTxnId == null || canRun(primaryTxnId, operation);
    }

    private boolean canRun(Object keyOrTxnId, AsyncOperation<?> operation)
    {
        Object operationOrQueue = queues.get(keyOrTxnId);
        Invariants.nonNull(operationOrQueue);

        if (operationOrQueue instanceof AsyncOperation<?>)
        {
            Invariants.checkState(operationOrQueue == operation);
            return true;
        }

        @SuppressWarnings("unchecked")
        ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
        return queue.peek() == operation;
    }
}
