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
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.primitives.Seekable;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.agrona.collections.Object2ObjectHashMap;
import org.apache.cassandra.service.accord.RangeTreeRangeAccessor;
import org.apache.cassandra.utils.RTree;
import org.apache.cassandra.utils.RangeTree;

/**
 * Assists with correct ordering of {@link AsyncOperation} execution wrt each other,
 * preventing reordering of overlapping operations by {@link AsyncLoader}.
 */
public class ExecutionOrder
{
    private static class Conflicts
    {
        private final List<Key> keyConflicts;
        private final List<Range> rangeConflicts;

        private Conflicts(List<Key> keyConflicts, List<Range> rangeConflicts)
        {
            this.keyConflicts = keyConflicts;
            this.rangeConflicts = rangeConflicts;
        }
    }
    private class RangeState
    {
        private final Range range;
        private final IdentityHashMap<AsyncOperation<?>, Conflicts> operationToConflicts = new IdentityHashMap<>();
        private Object operationOrQueue;

        public RangeState(Range range, List<Key> keyConflicts, List<Range> rangeConflicts, AsyncOperation<?> operation)
        {
            this.range = range;
            this.operationOrQueue = operation;
            add(operation, keyConflicts, rangeConflicts);
        }

        public void add(AsyncOperation<?> operation, List<Key> keyConflicts, List<Range> rangeConflicts)
        {
            operationToConflicts.put(operation, new Conflicts(keyConflicts, rangeConflicts));
        }

        boolean canRun(AsyncOperation<?> operation)
        {
            if (operationOrQueue instanceof AsyncOperation<?>)
            {
                Invariants.checkState(operationOrQueue == operation);
                return true;
            }
            else
            {
                ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
                return queue.peek() == operation;
            }
        }

        Conflicts remove(AsyncOperation<?> operation)
        {
            if (operationOrQueue instanceof AsyncOperation<?>)
            {
                Invariants.checkState(operationOrQueue == operation);
                rangeQueues.remove(range);
            }
            else
            {
                @SuppressWarnings("unchecked")
                ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
                AsyncOperation<?> head = queue.poll();
                Invariants.checkState(head == operation);

                if (queue.isEmpty())
                {
                    rangeQueues.remove(range);
                }
                else
                {
                    head = queue.peek();
                    if (canRun(head))
                        head.onUnblocked();
                }
            }
            return operationToConflicts.remove(operation);
        }

        public Conflicts conflicts(AsyncOperation<?> operation)
        {
            return operationToConflicts.get(operation);
        }
    }

    private final Object2ObjectHashMap<Object, Object> queues = new Object2ObjectHashMap<>();
    private final RangeTree<RoutingKey, Range, RangeState> rangeQueues = RTree.create(RangeTreeRangeAccessor.instance);

    /**
     * Register an operation as having a dependency on its keys and TxnIds
     * @return true if no other operation depends on the keys or TxnIds, false otherwise
     */
    boolean register(AsyncOperation<?> operation)
    {
        boolean canRun = true;
        for (Seekable seekable : operation.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    canRun &= register(seekable.asKey(), operation);
                    break;
                case Range:
                    canRun &= register(seekable.asRange(), operation);
                    break;
                default:
                    throw new AssertionError("Unexpected domain: " + seekable.domain());
            }
        }
        TxnId primaryTxnId = operation.primaryTxnId();
        if (null != primaryTxnId)
            canRun &= register(primaryTxnId, operation);
        return canRun;
    }

    private boolean register(Range range, AsyncOperation<?> operation)
    {
        // Ranges depend on Ranges and Keys
        // Keys depend on Keys...
        // This adds a complication to this logic as keys should be able to make progress regardless of ranges, but rangest must depend on keys
        List<Key> keyConflicts = null;
        for (Object o : queues.keySet())
        {
            if (!(o instanceof Key))
                continue;
            Key key = (Key) o;
            if (!range.contains(key))
                continue;
            if (keyConflicts == null)
                keyConflicts = new ArrayList<>();
            keyConflicts.add(key);
        }
        if (keyConflicts != null)
            keyConflicts.forEach(k -> register(k, operation));

        class Result
        {
            RangeState sameRange = null;
            List<Range> rangeConflicts = null;
        }
        Result result = new Result();
        rangeQueues.search(range, e -> {
            if (range.equals(e.getKey()))
                result.sameRange = e.getValue();
            else
            {
                if (result.rangeConflicts == null)
                    result.rangeConflicts = new ArrayList<>();
                result.rangeConflicts.add(e.getKey());
            }
            RangeState state = e.getValue();
            Object operationOrQueue = state.operationOrQueue;
            if (operationOrQueue instanceof AsyncOperation)
            {
                ArrayDeque<AsyncOperation<?>> queue = new ArrayDeque<>(4);
                queue.add((AsyncOperation<?>) operationOrQueue);
                queue.add(operation);
                state.operationOrQueue = queue;
            }
            else
            {
                @SuppressWarnings("unchecked")
                ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
                queue.add(operation);
            }
        });
        if (result.sameRange != null)
        {
            result.sameRange.add(operation, keyConflicts, result.rangeConflicts);
        }
        else
        {
            rangeQueues.add(range, new RangeState(range, keyConflicts, result.rangeConflicts, operation));
        }
        return keyConflicts == null && result.rangeConflicts == null;
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
        for (Seekable seekable : operation.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    unregister(seekable.asKey(), operation);
                    break;
                case Range:
                    unregister(seekable.asRange(), operation);
                    break;
                default:
                    throw new AssertionError("Unexpected domain: " + seekable.domain());
            }

        }
        TxnId primaryTxnId = operation.primaryTxnId();
        if (null != primaryTxnId)
            unregister(primaryTxnId, operation);
    }

    private void unregister(Range range, AsyncOperation<?> operation)
    {
        var state = state(range);
        var conflicts = state.remove(operation);
        if (conflicts.rangeConflicts != null)
            conflicts.rangeConflicts.forEach(r -> state(r).remove(operation));
        if (conflicts.keyConflicts != null)
            conflicts.keyConflicts.forEach(k -> unregister(k, operation));
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
        for (Seekable seekable : operation.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    if (!canRun(seekable.asKey(), operation))
                        return false;
                    break;
                case Range:
                    if (!canRun(seekable.asRange(), operation))
                        return false;
                    break;
                default:
                    throw new AssertionError("Unexpected domain: " + seekable.domain());
            }

        }

        TxnId primaryTxnId = operation.primaryTxnId();
        return primaryTxnId == null || canRun(primaryTxnId, operation);
    }

    private boolean canRun(Range range, AsyncOperation<?> operation)
    {
        var state = state(range);
        if (!state.canRun(operation))
            return false;
        var conflicts = state.conflicts(operation);
        if (conflicts.rangeConflicts != null)
        {
            for (var r : conflicts.rangeConflicts)
            {
                if (!state(r).canRun(operation))
                    return false;
            }
        }
        if (conflicts.keyConflicts != null)
        {
            for (Key key : conflicts.keyConflicts)
            {
                if (!canRun(key, operation))
                    return false;
            }
        }
        return true;
    }

    private RangeState state(Range range)
    {
        var list = rangeQueues.get(range);
        assert list.size() == 1 : String.format("Expected 1 element but saw list %s", list);
        return list.get(0);
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
