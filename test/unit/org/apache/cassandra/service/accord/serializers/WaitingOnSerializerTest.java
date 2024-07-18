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

package org.apache.cassandra.service.accord.serializers;

import java.nio.ByteBuffer;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.Command;
import accord.primitives.Deps;
import accord.primitives.Routable;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.SimpleBitSet;
import accord.utils.Utils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class WaitingOnSerializerTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void serde()
    {
        qt().forAll(waitingOnGen()).check(waitingOn -> {
            TxnId txnId = TxnId.NONE;
            if (waitingOn.appliedOrInvalidated != null) txnId = new TxnId(txnId.epoch(), txnId.hlc(), txnId.kind(), Routable.Domain.Range, txnId.node);
            long expectedSize = WaitingOnSerializer.serializedSize(waitingOn);
            ByteBuffer bb = WaitingOnSerializer.serialize(txnId, waitingOn);
            Assertions.assertThat(bb.remaining()).isEqualTo(expectedSize);
            Command.WaitingOn read = WaitingOnSerializer.deserialize(txnId, waitingOn.keys, waitingOn.directRangeDeps, waitingOn.directKeyDeps, bb);
            Assertions.assertThat(read)
                      .isEqualTo(waitingOn)
                      .isEqualTo(WaitingOnSerializer.deserialize(txnId, waitingOn.keys, waitingOn.directRangeDeps, waitingOn.directKeyDeps, WaitingOnSerializer.serialize(txnId, waitingOn)));
        });
    }

    private enum WaitingOnSets { APPLY, APPLIED_OR_INVALIDATED }

    private static Gen<Command.WaitingOn> waitingOnGen()
    {
        Gen<Deps> depsGen = AccordGenerators.fromQT(CassandraGenerators.PARTITIONER_GEN)
                                            .filter(i -> !(i instanceof LocalPartitioner))
                                            .flatMap(AccordGenerators::depsGen);
        Gen<WaitingOnSets> sets = Gens.enums().all(WaitingOnSets.class);
        return rs -> {
            Deps deps = depsGen.next(rs);
            if (deps.isEmpty()) return Command.WaitingOn.EMPTY;
            int txnIdCount = deps.rangeDeps.txnIdCount() + deps.directKeyDeps.txnIdCount();
            int keyCount = deps.keyDeps.keys().size();
            int[] selected = Gens.arrays(Gens.ints().between(0, txnIdCount + keyCount - 1)).unique().ofSizeBetween(0, txnIdCount + keyCount).next(rs);
            SimpleBitSet waitingOn = new SimpleBitSet(txnIdCount + keyCount, false);
            SimpleBitSet appliedOrInvalidated = rs.nextBoolean() ? null : new SimpleBitSet(txnIdCount, false);
            for (int i : selected)
            {
                WaitingOnSets set = appliedOrInvalidated == null || i >= txnIdCount ? WaitingOnSets.APPLY : sets.next(rs);
                switch (set)
                {
                    case APPLY:
                        waitingOn.set(i);
                        break;
                    case APPLIED_OR_INVALIDATED:
                        appliedOrInvalidated.set(i);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected set: " + set);
                }
            }

            return new Command.WaitingOn(deps.keyDeps.keys(), deps.rangeDeps, deps.directKeyDeps, Utils.ensureImmutable(waitingOn), Utils.ensureImmutable(appliedOrInvalidated));
        };
    }
}