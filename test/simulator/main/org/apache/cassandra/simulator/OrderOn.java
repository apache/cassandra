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

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

// TODO (now): javadoc
@Shared(scope = SIMULATION)
public interface OrderOn
{
    public static final OrderOn NONE = new OrderOn()
    {
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

    int concurrency();
    default boolean isStrict() { return false; }
    default boolean isOrdered() { return concurrency() < Integer.MAX_VALUE; }

    abstract class OrderOnId implements OrderOn
    {
        public final Object id;

        public OrderOnId(Object id)
        {
            this.id = id;
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
}
