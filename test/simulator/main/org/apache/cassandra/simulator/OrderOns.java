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

import java.util.ArrayList;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * A (possibly empty) collection of OrderOn
 */
@Shared(scope = SIMULATION)
public interface OrderOns
{
    /**
     * Equivalent to !isEmpty()
     */
    boolean isOrdered();

    /**
     * Equivalent to anyMatch(OrderOn::isOrdered)
     */
    boolean isStrict();

    /**
     * Return an {@code OrderOns} (possibly this one) also containing {@code add}
     */
    OrderOns with(OrderOn add);

    /**
     * The number of {@link OrderOn} contained in this collection
     */
    int size();

    /**
     * The i'th {@link OrderOn} contained in this collection
     */
    OrderOn get(int i);

    public static final OrderOn NONE = new OrderOn()
    {
        @Override
        public OrderOns with(OrderOn add)
        {
            return add;
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public OrderOn get(int i)
        {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public int concurrency()
        {
            return Integer.MAX_VALUE;
        }

        @Override
        public String toString()
        {
            return "Unordered";
        }
    };

    public class TwoOrderOns implements OrderOns
    {
        final OrderOn one;
        final OrderOn two;

        public TwoOrderOns(OrderOn one, OrderOn two)
        {
            this.one = one;
            this.two = two;
        }

        @Override
        public boolean isOrdered()
        {
            return true;
        }

        @Override
        public boolean isStrict()
        {
            return one.isStrict() || two.isStrict();
        }

        @Override
        public OrderOns with(OrderOn three)
        {
            OrderOnsList result = new OrderOnsList();
            result.add(one);
            result.add(two);
            result.add(three);
            return result;
        }

        @Override
        public int size()
        {
            return 2;
        }

        @Override
        public OrderOn get(int i)
        {
            Preconditions.checkArgument((i & 1) == i);
            return i == 0 ? one : two;
        }
    }

    public class OrderOnsList extends ArrayList<OrderOn> implements OrderOns
    {
        @Override
        public boolean isOrdered()
        {
            return true;
        }

        @Override
        public boolean isStrict()
        {
            for (int i = 0 ; i < size() ; ++i)
            {
                if (get(i).isStrict())
                    return true;
            }
            return false;
        }

        public OrderOns with(OrderOn add)
        {
            add(add);
            return this;
        }
    }
}
