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

package org.apache.cassandra.simulator;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * A token representing some ordering property of the system.
 * Most notably this is used to implement executor services where a number of tasks may be run concurrently,
 * particularly single threaded executors where causality is a strict requirement of correctness.
 *
 * This is also used to denote "strict" ordering on any suitably annotated {@link Action}.
 *
 * For convenience, efficiency and simplicity we have OrderOn represent a singleton collection of OrderOns.
 */
@Shared(scope = SIMULATION)
public interface OrderOn extends OrderOns
{
    /**
     * The number of {@link Action} in the provided sequence that may be executed at once,
     * before any ordering takes effect.
     */
    int concurrency();

    /**
     * If true then all child actions (and their children, etc) must be ordered together, i.e. the next
     * {@link Action} ordered by this sequence may not run until the present {@link Action} and all actions
     * started by it, directly or indirectly, have completed.
     */
    default boolean isStrict() { return false; }

    /**
     * Whether the ordering is imposed immediately, occupying a slot in the sequence prior to any Action being scheduled
     * (e.g. in the case of {@code executor.execute()}), or if it applies only after the scheduled time elapsed
     * (e.g. in the case of {@code executor.schedule()}).
     *
     * This may be modified and still refer to the same {@code OrderOn} as another {@code OrderOn} by overriding
     * the {@code unwrap()} method
     */
    default boolean appliesBeforeScheduling() { return true; }

    /**
     * {@code this} may be a thin wrapper around another {@code OrderOn} with a different {@code appliesBeforeScheduling()}.
     * In this case this method returns the underlying {@code OrderOn} to impose the order upon.
     */
    default OrderOn unwrap() { return this; }

    /**
     * A convenience method to indicate if this {@code OrderOn} imposes any ordering
     */
    @Override
    default boolean isOrdered() { return concurrency() < Integer.MAX_VALUE; }

    @Override
    default OrderOns with(OrderOn add)
    {
        return new TwoOrderOns(this, add);
    }

    @Override
    default int size()
    {
        return 1;
    }

    @Override
    default OrderOn get(int i)
    {
        Preconditions.checkArgument(i == 0);
        return this;
    }

    abstract class OrderOnId implements OrderOn
    {
        public final Object id;

        public OrderOnId(Object id)
        {
            this.id = id;
        }

        @Override
        public int hashCode()
        {
            return id.hashCode();
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof OrderOnId && id.equals(((OrderOnId) that).id);
        }

        public String toString()
        {
            return id.toString();
        }
    }

    public class Sequential extends OrderOnId
    {
        public Sequential(Object id)
        {
            super(id);
        }

        public int concurrency() { return 1; }
    }

    public class StrictSequential extends Sequential
    {
        public StrictSequential(Object id)
        {
            super(id);
        }

        @Override
        public boolean isStrict()
        {
            return true;
        }
    }

    public class Strict extends Sequential
    {
        final int concurrency;

        public Strict(Object id, int concurrency)
        {
            super(id);
            this.concurrency = concurrency;
        }

        @Override
        public int concurrency()
        {
            return concurrency;
        }

        @Override
        public boolean isStrict()
        {
            return true;
        }
    }

    public class OrderAppliesAfterScheduling implements OrderOn
    {
        final OrderOn inner;

        public OrderAppliesAfterScheduling(OrderOn inner) { this.inner = inner; }
        @Override public int concurrency() { return inner.concurrency(); }
        @Override public boolean isStrict() { return inner.isStrict(); }
        @Override public boolean isOrdered() { return inner.isOrdered(); }

        @Override public boolean appliesBeforeScheduling() { return false; }
        @Override public OrderOn unwrap() { return inner; }
        @Override public String toString() { return inner.toString(); }
    }
}
