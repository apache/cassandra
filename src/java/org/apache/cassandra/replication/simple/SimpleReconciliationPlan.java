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

package org.apache.cassandra.replication.simple;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;

import org.apache.cassandra.db.MutationId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ReconciliationPlan;
import org.apache.cassandra.utils.CollectionSerializer;

import static org.apache.cassandra.locator.InetAddressAndPort.Serializer.inetAddressAndPortSerializer;

public class SimpleReconciliationPlan implements ReconciliationPlan
{
    private final ImmutableMap<InetAddressAndPort, ImmutableSet<MutationId>> txPlan;

    public SimpleReconciliationPlan(ImmutableMap<InetAddressAndPort, ImmutableSet<MutationId>> txPlan)
    {
        this.txPlan = txPlan;
    }

    @Override
    public Set<InetAddressAndPort> nodes()
    {
        return txPlan.keySet();
    }

    @Override
    public Set<MutationId> idsFor(InetAddressAndPort node)
    {
        return txPlan.get(node);
    }

    private static class Builder
    {
        final InetAddressAndPort node;
        private final ImmutableSet<MutationId> owned;

        private final Map<InetAddressAndPort, Set<MutationId>> txPlan = new HashMap<>();

        public Builder(InetAddressAndPort node, SimpleMutationSummary summary)
        {
            this.node = node;

            ImmutableSet.Builder<MutationId> builder = ImmutableSet.builder();
            for (ImmutableSortedSet<MutationId> ids : summary.ids.values())
                builder.addAll(ids);

            this.owned = builder.build();
        }

        void send(InetAddressAndPort to, MutationId id)
        {
            Preconditions.checkState(!node.equals(to));
            Preconditions.checkArgument(owned.contains(id));
            txPlan.computeIfAbsent(node, k -> Sets.newHashSet()).add(id);
        }

        SimpleReconciliationPlan build()
        {
            ImmutableMap.Builder<InetAddressAndPort, ImmutableSet<MutationId>> builder = ImmutableMap.builder();
            txPlan.forEach((peer, ids) -> builder.put(peer, ImmutableSet.copyOf(ids)));
            return new SimpleReconciliationPlan(builder.build());
        }
    }

    public static Map<InetAddressAndPort, ReconciliationPlan> calculateReconciliation(Map<InetAddressAndPort, MutationSummary> summaries)
    {
        Map<InetAddressAndPort, Builder> plans = new HashMap<>();
        summaries.forEach((node, summary)
                          -> plans.put(node, new Builder(node, (SimpleMutationSummary) summary))
        );

        ImmutableSet<MutationId> allIds;
        {
            ImmutableSet.Builder<MutationId> builder = ImmutableSet.builder();
            for (Builder plan : plans.values())
                builder.addAll(plan.owned);
            allIds = builder.build();
        }

        for (Builder receiver : plans.values())
        {
            for (MutationId missing : Sets.difference(allIds, receiver.owned))
            {
                for (Builder sender : plans.values())
                {
                    if (sender == receiver)
                        continue;

                    if (sender.owned.contains(missing))
                        sender.send(receiver.node, missing);
                }
            }
        }

        ImmutableMap.Builder<InetAddressAndPort, ReconciliationPlan> result = ImmutableMap.builder();
        plans.values().forEach(plan -> result.put(plan.node, plan.build()));
        return result.build();
    }

    public static final IVersionedSerializer<SimpleReconciliationPlan> serializer = new IVersionedSerializer<SimpleReconciliationPlan>()
    {
        @Override
        public void serialize(SimpleReconciliationPlan plan, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(plan.txPlan.size());
            for (Map.Entry<InetAddressAndPort, ImmutableSet<MutationId>> entry : plan.txPlan.entrySet())
            {
                inetAddressAndPortSerializer.serialize(entry.getKey(), out, version);
                CollectionSerializer.serializeCollection(MutationId.serializer, entry.getValue(), out, version);
            }
        }

        @Override
        public SimpleReconciliationPlan deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            ImmutableMap.Builder<InetAddressAndPort, ImmutableSet<MutationId>> builder = ImmutableMap.builder();
            for (int i = 0; i < size; i++)
            {
                InetAddressAndPort node = inetAddressAndPortSerializer.deserialize(in, version);
                Set<MutationId> idset = CollectionSerializer.deserializeCollection(MutationId.serializer, s -> new HashSet<>(), in, version);
                builder.put(node, ImmutableSet.copyOf(idset));
            }
            return new SimpleReconciliationPlan(builder.build());
        }

        @Override
        public long serializedSize(SimpleReconciliationPlan plan, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(plan.txPlan.size());
            for (Map.Entry<InetAddressAndPort, ImmutableSet<MutationId>> entry : plan.txPlan.entrySet())
            {
                size += inetAddressAndPortSerializer.serializedSize(entry.getKey(), version);
                size += CollectionSerializer.serializedSizeCollection(MutationId.serializer, entry.getValue(), version);
            }
            return size;
        }
    };
}
