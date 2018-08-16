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

package org.apache.cassandra.service.reads.repair;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;

public enum ReadRepairStrategy implements ReadRepair.Factory
{
    NONE
    {
        @Override
        public ReadRepair create(ReadCommand command, long queryStartNanoTime, ConsistencyLevel consistency)
        {
            return new ReadOnlyReadRepair(command, queryStartNanoTime, consistency);
        }
    },

    BLOCKING
    {
        @Override
        public ReadRepair create(ReadCommand command, long queryStartNanoTime, ConsistencyLevel consistency)
        {
            return new BlockingReadRepair(command, queryStartNanoTime, consistency);
        }
    };

    public static ReadRepairStrategy fromString(String s)
    {
        return valueOf(s.toUpperCase());
    }
}
