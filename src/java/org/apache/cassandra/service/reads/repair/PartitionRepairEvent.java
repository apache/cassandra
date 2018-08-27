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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.ByteBufferUtil;

final class PartitionRepairEvent extends DiagnosticEvent
{
    private final PartitionRepairEventType type;
    @VisibleForTesting
    final InetAddressAndPort destination;
    @Nullable
    private final Keyspace keyspace;
    @Nullable
    private final DecoratedKey key;
    @Nullable
    private final ConsistencyLevel consistency;
    @Nullable
    @VisibleForTesting
    String mutationSummary;

    enum PartitionRepairEventType
    {
        SEND_INITIAL_REPAIRS,
        SPECULATED_WRITE,
        UPDATE_OVERSIZED
    }

    PartitionRepairEvent(PartitionRepairEventType type, BlockingPartitionRepair partitionRepair,
                         InetAddressAndPort destination, Mutation mutation)
    {
        this.type = type;
        this.destination = destination;
        this.keyspace = partitionRepair.getKeyspace();
        this.consistency = partitionRepair.getConsistency();
        this.key = partitionRepair.getKey();
        if (mutation != null)
        {
            try
            {
                this.mutationSummary = mutation.toString();
            }
            catch (Exception e)
            {
                this.mutationSummary = String.format("<Mutation.toString(): %s>", e.getMessage());
            }
        }
    }

    public PartitionRepairEventType getType()
    {
        return type;
    }

    public Map<String, Serializable> toMap()
    {
        HashMap<String, Serializable> ret = new HashMap<>();
        if (keyspace != null) ret.put("keyspace", keyspace.getName());
        if (key != null)
        {
            ret.put("key", key.getKey() == null ? "null" : ByteBufferUtil.bytesToHex(key.getKey()));
            ret.put("token", key.getToken().toString());
        }
        if (consistency != null) ret.put("consistency", consistency.name());

        ret.put("destination", destination.toString());

        if (mutationSummary != null) ret.put("mutation", mutationSummary);

        return ret;
    }
}
