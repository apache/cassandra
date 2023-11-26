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

package org.apache.cassandra.debug;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.Replica;

public class BlockingReadRepairDebugLog
{
    private static final Logger logger = LoggerFactory.getLogger(BlockingReadRepairDebugLog.class);

    // It is important to provide the key for the data we are interested so it's easier to filter in uMonitor logs
    public static void info(DecoratedKey key, String message)
    {
        if (DatabaseDescriptor.getBlockingReadRepairDebugLogEnabled())
        {
            logger.info(String.format("Read repair debug for key %s: %s", key == null ? "Not provided" : key, message));
        }
    }

    public static void logPendingRepairs(DecoratedKey key, Map<Replica, Mutation> pendingRepairs)
    {
        if (DatabaseDescriptor.getBlockingReadRepairDebugLogEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for (Replica replica : pendingRepairs.keySet())
            {
                sb.append(replica.endpoint());
                sb.append(",");
            }
            info(key, String.format("initial pending repairs %s", sb));
        }
    }

    public static void logTimestampOfMergedCells(DecoratedKey key, Row merged)
    {
        if (DatabaseDescriptor.getBlockingReadRepairDebugLogEnabled())
        {
            long latestTimeStamp = 0L;
            for (Cell cell : merged.cells())
            {
                latestTimeStamp = latestTimeStamp > cell.timestamp() ? latestTimeStamp : cell.timestamp();
            }
            info(key, String.format("The latest write timestamp of the merged cell %d", latestTimeStamp));
        }
    }
}
