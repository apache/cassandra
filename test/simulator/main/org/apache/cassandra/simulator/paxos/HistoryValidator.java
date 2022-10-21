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

package org.apache.cassandra.simulator.paxos;

import javax.annotation.Nullable;

public interface HistoryValidator
{
    Checker witness(int start, int end);

    void print(@Nullable Integer pk);

    interface Checker extends AutoCloseable
    {
        void read(int pk, int id, int count, int[] seq);
        void write(int pk, int id, boolean success);

        default void writeSuccess(int pk, int id)
        {
            write(pk, id, true);
        }

        default void writeUnknownFailure(int pk, int id)
        {
            write(pk, id, false);
        }

        @Override
        default void close() {}
    }

    interface Factory
    {
        HistoryValidator create(int[] partitions);
    }
}
