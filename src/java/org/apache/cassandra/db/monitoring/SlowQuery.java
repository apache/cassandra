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

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.service.MonitoringService;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class SlowQuery extends BadQueryTypes
{
    long latencyInms;
    private String keyColumns;

    public SlowQuery(String keySpace,
                     String tableName,
                     String keyColumns,
                     long latencyInms)
    {
        super(keySpace, tableName);
        this.keyColumns = keyColumns;
        this.latencyInms = latencyInms;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", key:");
        sb.append(keyColumns);
        sb.append(", latency:");
        sb.append(latencyInms);
        sb.append("ms");
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
        sb.append("latency:");
        sb.append(latencyInms);
        sb.append("ms");
        return sb.toString();
    }

    static void checkForSlowCoordinatorWrite(Collection<? extends IMutation> mutations, long durationInns)
    {
        long latencyInms = TimeUnit.NANOSECONDS.toMillis(durationInns);
        if (latencyInms > MonitoringService.instance.getBadQueryWriteSlowCoordLatencyInms())
        {
            StringBuilder keyspace = new StringBuilder();
            StringBuilder table = new StringBuilder();
            StringBuilder key = new StringBuilder();
            boolean firstRound = true;
            for (IMutation mutation : mutations)
            {
                if (!firstRound)
                {
                    keyspace.append(",");
                    table.append(",");
                    key.append(",");
                }
                firstRound = false;
                keyspace.append(mutation.getKeyspaceName());
                table.append(StringUtils.join(mutation.getTableNames(), ":"));
                key.append(mutation.getKey());
            }
            BadQuery.report(BadQuery.BadQueryCategory.SLOW_WRITE_COORINATOR, new SlowQuery(keyspace.toString(),
                                                                                           table.toString(), key.toString(), latencyInms));
        }
    }

    static void checkForSlowLocalRead(ReadCommand command,
                                      long durationInns)
    {
        long latencyInms = TimeUnit.NANOSECONDS.toMillis(durationInns);

        if (latencyInms > MonitoringService.instance.getBadQueryReadSlowLocalLatencyInms())
        {
            String key = command.getKey();
            BadQuery.report(BadQuery.BadQueryCategory.SLOW_READ_LOCAL,
                            new SlowQuery(command.metadata().keyspace, command.metadata().name, key, latencyInms));
        }
    }

    static void checkForSlowCoordinatorRead(List<SinglePartitionReadCommand> commands,
                                            long latencyinns)
    {
        long latencyInms = TimeUnit.NANOSECONDS.toMillis(latencyinns);
        if (latencyInms > MonitoringService.instance.getBadQueryReadSlowCoordLatencyInms())
        {
            Set<String> keyspaceName = new TreeSet<>();
            Set<String> tableName = new TreeSet<>();
            Set<String> key = new TreeSet<>();
            for (ReadCommand command : commands)
            {
                key.add(command.getKey());
                keyspaceName.add(command.metadata().keyspace);
                tableName.add(command.metadata().name);
            }
            BadQuery.report(BadQuery.BadQueryCategory.SLOW_READ_COORINATOR,
                            new SlowQuery(String.join(":", keyspaceName), String.join(":", tableName), String.join(":", key.toString()), latencyInms));
        }
    }
}

