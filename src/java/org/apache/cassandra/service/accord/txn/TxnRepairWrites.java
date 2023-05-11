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

import java.util.Collection;

import com.google.common.collect.ForwardingCollection;

import accord.api.RepairWrites;
import accord.api.Write;
import accord.primitives.Keys;
import accord.primitives.Seekables;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static accord.utils.Invariants.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class TxnRepairWrites extends ForwardingCollection<PartitionUpdate> implements RepairWrites
{
    private final Collection<PartitionUpdate> repairWrites;

    public TxnRepairWrites(Collection<PartitionUpdate> repairWrites)
    {
        checkArgument(repairWrites != null && !repairWrites.isEmpty());
        this.repairWrites = repairWrites;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return Keys.of(repairWrites.stream()
                                   .map(PartitionKey::of)
                                   .collect(toImmutableList()));
    }

    @Override
    public Write toWrite()
    {
        return TxnUpdate.txnRepairWritesToWrite(this);
    }

    @Override
    protected Collection<PartitionUpdate> delegate()
    {
        return repairWrites;
    }
}
