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

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.ReplicaPlan;

public enum ReadRepairStrategy implements ReadRepair.Factory
{
    NONE
    {
        public <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        ReadRepair<E, P> create(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
        {
            return new ReadOnlyReadRepair<>(command, replicaPlan, queryStartNanoTime);
        }
    },

    BLOCKING
    {
        public <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        ReadRepair<E, P> create(ReadCommand command, ReplicaPlan.Shared<E, P> replicaPlan, long queryStartNanoTime)
        {
            return new BlockingReadRepair<>(command, replicaPlan, queryStartNanoTime);
        }
    };

    public static ReadRepairStrategy fromString(String s)
    {
        return valueOf(s.toUpperCase());
    }
}
