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

package org.apache.cassandra.service.accord.txn;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;

public class AccordUpdateParameters
{
    private final TxnData data;
    private final QueryOptions options;

    public AccordUpdateParameters(TxnData data, QueryOptions options)
    {
        this.data = data;
        this.options = options;
    }

    public TxnData getData()
    {
        return data;
    }

    public UpdateParameters updateParameters(TableMetadata metadata, int rowIndex)
    {
        // This is currently only used by Guardrails, but this logically have issues with Accord as drifts in config
        // values could cause unexpected issues in Accord. (ex. some nodes reject writes while others accept)
        // For the time being, guardrails are disabled for Accord queries.
        ClientState disabledGuardrails = null;

        // What we use here doesn't matter as they get replaced before actually performing the write.
        // see org.apache.cassandra.service.accord.txn.TxnWrite.Update.write
        int nowInSeconds = 42;
        long timestamp = nowInSeconds;

        // TODO: How should Accord work with TTL?
        int ttl = metadata.params.defaultTimeToLive;
        return new UpdateParameters(metadata,
                                    disabledGuardrails,
                                    options,
                                    timestamp,
                                    nowInSeconds,
                                    ttl,
                                    prefetchRow(metadata, rowIndex));
    }

    private Map<DecoratedKey, Partition> prefetchRow(TableMetadata metadata, int index)
    {
        for (Map.Entry<TxnDataName, FilteredPartition> e : data.entrySet())
        {
            TxnDataName name = e.getKey();
            if (name.isAutoRead() && name.atIndex(index))
                return ImmutableMap.of(name.getDecoratedKey(metadata), e.getValue());
        }
        return Collections.emptyMap();
    }
}
