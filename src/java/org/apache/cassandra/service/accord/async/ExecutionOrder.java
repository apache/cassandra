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
import java.util.function.Consumer;

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

        Conflicts remove(AsyncOperation<?> operation, boolean allowOutOfOrder)
        {
            unregister(operationOrQueue, operation, allowOutOfOrder, () -> rangeQueues.remove(range));
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
            // a single range could conflict with multiple other ranges, so it is possible that the operation
            // exists in the queue already due to another range in the txn... simple example is
            // keys = (0, 10], (12, 15]
            // e.getKey() == (-100, 100]
            // in this case the operation would attempt to double add since it has 2 keys that conflict with this single range
            register(state.operationOrQueue, operation, q -> state.operationOrQueue = q);
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

        register(operationOrQueue, operation, q -> queues.put(keyOrTxnId, q));
        return false;
    }

    private void register(Object operationOrQueue, AsyncOperation<?> operation, Consumer<ArrayDeque<AsyncOperation<?>>> onCreateQueue)
    {
        if (operationOrQueue instanceof AsyncOperation)
        {
            Invariants.checkState(operationOrQueue != operation, "Attempted to double register operation %s", operation);
            ArrayDeque<AsyncOperation<?>> queue = new ArrayDeque<>(4);
            queue.add((AsyncOperation<?>) operationOrQueue);
            queue.add(operation);
            onCreateQueue.accept(queue);
        }
        else
        {
            @SuppressWarnings("unchecked")
            ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
            queue.add(operation);
        }
    }

    /**
     * Unregister the operation as being a dependency for its keys and TxnIds, but do so even if it is unable to run now.
     */
    void unregisterOutOfOrder(AsyncOperation<?> operation)
    {
        unregister(operation, true);
    }

    /**
     * Unregister the operation as being a dependency for its keys and TxnIds
     */
    void unregister(AsyncOperation<?> operation)
    {
        unregister(operation, false);
    }

    private void unregister(AsyncOperation<?> operation, boolean allowOutOfOrder)
    {
        for (Seekable seekable : operation.keys())
        {
            switch (seekable.domain())
            {
                case Key:
                    unregister(seekable.asKey(), operation, allowOutOfOrder);
                    break;
                case Range:
                    unregister(seekable.asRange(), operation, allowOutOfOrder);
                    break;
                default:
                    throw new AssertionError("Unexpected domain: " + seekable.domain());
            }

        }
        TxnId primaryTxnId = operation.primaryTxnId();
        if (null != primaryTxnId)
            unregister(primaryTxnId, operation, allowOutOfOrder);
    }

    private void unregister(Range range, AsyncOperation<?> operation, boolean allowOutOfOrder)
    {
        var state = state(range);
        var conflicts = state.remove(operation, allowOutOfOrder);
        if (conflicts.rangeConflicts != null)
            conflicts.rangeConflicts.forEach(r -> state(r).remove(operation, allowOutOfOrder));
        if (conflicts.keyConflicts != null)
            conflicts.keyConflicts.forEach(k -> unregister(k, operation, allowOutOfOrder));
    }

    /**
     * Unregister the operation as being a dependency for key or TxnId
     */
    private void unregister(Object keyOrTxnId, AsyncOperation<?> operation, boolean allowOutOfOrder)
    {
        Object operationOrQueue = queues.get(keyOrTxnId);
        Invariants.nonNull(operationOrQueue);

        unregister(operationOrQueue, operation, allowOutOfOrder, () -> queues.remove(keyOrTxnId));
    }

    private void unregister(Object operationOrQueue, AsyncOperation<?> operation, boolean allowOutOfOrder, Runnable onEmpty)
    {
        if (operationOrQueue instanceof AsyncOperation<?>)
        {
            Invariants.checkState(operationOrQueue == operation);
            onEmpty.run();
        }
        else
        {
            @SuppressWarnings("unchecked")
            ArrayDeque<AsyncOperation<?>> queue = (ArrayDeque<AsyncOperation<?>>) operationOrQueue;
            if (allowOutOfOrder)
            {
                Invariants.checkState(queue.remove(operation), "Operation %s was not found in queue: %s", operation, queue);
            }
            else
            {
                Invariants.checkState(queue.peek() == operation, "Operation %s is not at the top of the queue; %s", operation, queue);
                queue.poll();
            }

            if (queue.isEmpty())
            {
                onEmpty.run();
            }
            else
            {
                AsyncOperation<?> next = queue.peek();
                if (next == operation)
                {
                    // a single range could conflict with multiple other ranges, so it is possible that the operation
                    // exists in the queue already due to another range in the txn... simple example is
                    // keys = (0, 10], (12, 15]
                    // e.getKey() == (-100, 100]
                    // in this case the operation would attempt to double add since it has 2 keys that conflict with this single range
                    return;
                }
                if (canRun(next))
                    next.onUnblocked();
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
        assert list.size() == 1 : String.format("Expected 1 element for range %s but saw list %s", range, list);
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
