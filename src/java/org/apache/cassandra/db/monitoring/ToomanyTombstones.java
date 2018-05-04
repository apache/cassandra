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
package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.service.MonitoringService;

public class ToomanyTombstones extends BadQueryTypes
{
    private int tombstones;
    private String keyColumns;

    public ToomanyTombstones(String keySpace,
                             String tableName,
                             String keyColumns,
                             int tombstones)
    {
        super(keySpace, tableName);
        this.keyColumns = keyColumns;
        this.tombstones = tombstones;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", key:");
        sb.append(keyColumns);
        sb.append(", tombstones:");
        sb.append(tombstones);
        return sb.toString();
    }

    @Override
    public String getKey()
    {
        return keyColumns;
    }

    @Override
    public String getDetails()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("tombstones:");
        sb.append(tombstones);
        return sb.toString();
    }

    static void checkForTooManyTombstones(ReadCommand command,
                                          int tombstones)
    {
        if (tombstones >= MonitoringService.instance.getBadQueryTombstoneLimit())
        {
            BadQuery.report(BadQuery.BadQueryCategory.TOO_MANY_TOMBSTONES,
                    new ToomanyTombstones(command.metadata().keyspace, command.metadata().name, command.getKey(), tombstones));
        }
    }
}

