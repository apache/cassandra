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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.MonitoringService;
import org.apache.commons.lang3.StringUtils;

public class LargePartition extends BadQueryTypes
{
    private long size;
    private String keyColumns;

    public LargePartition(String keySpace,
                          String tableName,
                          String keyColumns,
                          long size)
    {
        super(keySpace, tableName);
        this.keyColumns = keyColumns;
        this.size = size;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", key:");
        sb.append(keyColumns);
        sb.append(", size:");
        sb.append(size);
        sb.append("B");
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
        sb.append("size:");
        sb.append(size);
        sb.append("B");
        return sb.toString();
    }

    static void checkForLargeRead(ReadCommand readCommand,
                                  long size)
    {
        if (size > MonitoringService.instance.getBadQueryReadMaxPartitionSizeInbytes())
        {
            BadQuery.report(BadQuery.BadQueryCategory.LARGE_PARTITION_READ,
                    new LargePartition(readCommand.metadata().keyspace, readCommand.metadata().name, readCommand.getKey(), size));
        }
    }

    static void checkForLargeWrite(Mutation mutation,
                                   long size)
    {
        if (size > MonitoringService.instance.getBadQueryWriteMaxPartitionSizeInbytes())
        {
            BadQuery.report(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE,
                    new LargePartition(mutation.getKeyspaceName(), StringUtils.join(mutation.getTableNames(), ":"), mutation.getKey(), size));
        }
    }

    static void checkForLargeWrite(TableMetadata metadata,
                                   DecoratedKey decoratedKey,
                                   long size)
    {
        if (size > MonitoringService.instance.getBadQueryWriteMaxPartitionSizeInbytes())
        {
            BadQuery.report(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE,
                            new LargePartition(metadata.keyspace, metadata.name, metadata.partitionKeyType.getString(decoratedKey.getKey()), size));
        }
    }

    public long getSize()
    {
        return size;
    }
}

