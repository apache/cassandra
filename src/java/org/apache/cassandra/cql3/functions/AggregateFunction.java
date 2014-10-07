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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Performs a calculation on a set of values and return a single value.
 */
public interface AggregateFunction extends Function
{
    /**
     * Creates a new <code>Aggregate</code> instance.
     *
     * @return a new <code>Aggregate</code> instance.
     */
    public Aggregate newAggregate();

    /**
     * An aggregation operation.
     */
    interface Aggregate
    {
        /**
         * Adds the specified input to this aggregate.
         *
         * @param values the values to add to the aggregate.
         */
        public void addInput(List<ByteBuffer> values);

        /**
         * Computes and returns the aggregate current value.
         *
         * @return the aggregate current value.
         */
        public ByteBuffer compute();

        /**
         * Reset this aggregate.
         */
        public void reset();
    }
}
