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

package org.apache.cassandra.harry.dsl;

public interface PartitionVisitState
{
    /**
     * Informs a generator that a specific clustering key has to be generated.
     *
     * Since Harry model has a few constraints, we can not override to an arbitrary clustering to an arbitrary value, since
     * Harry ensures that clustering descriptors are sorted the same way clusterings themselves are sorted.
     *
     * Gladly, most of the time we need to override just one or a couple of values to trigger some edge condition,
     * which means that we can simply instruct Harry to produce a given handcrafted value.
     *
     * When using `ensureClustering`, you can not reliably know in advance where specifically in the row this value is
     * going to sort.
     *
     * If you need arbitrary overrides, you will have to produce _all_ clusterings possible for the given partition.
     */
    void ensureClustering(Object[] overrides);
    void overrideClusterings(Object[][] overrides);
    long pd();
}
