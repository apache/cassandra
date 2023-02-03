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

package org.apache.cassandra.utils;

import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import accord.local.Command;
import accord.local.Node;
import accord.primitives.PartialTxn;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.service.accord.AccordTestUtils;

import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;

public class AccordGenerators
{
    private AccordGenerators() {}

    public static Gen.LongGen epochs()
    {
        return Gens.longs().between(0, Timestamp.MAX_EPOCH);
    }

    public static Gen<TxnId> ids()
    {
        return ids(epochs()::nextLong, Gen.Random::nextLong, Gen.Random::nextInt);
    }

    public static Gen<TxnId> ids(ToLongFunction<Gen.Random> epochs, ToLongFunction<Gen.Random> hlcs, ToIntFunction<Gen.Random> nodes)
    {
        Gen<Txn.Kind> kinds = Gens.enums().all(Txn.Kind.class);
        Gen<Routable.Domain> domains = Gens.enums().all(Routable.Domain.class);
        return rs -> new TxnId(epochs.applyAsLong(rs), hlcs.applyAsLong(rs), kinds.next(rs), domains.next(rs), new Node.Id(nodes.applyAsInt(rs)));
    }

    private enum SupportedCommandTypes { notWitnessed, preaccepted, committed }

    public static Gen<Command> commands()
    {
        Gen<TxnId> ids = ids();
        //TODO switch to Status once all types are supported
        Gen<SupportedCommandTypes> supportedTypes = Gens.enums().all(SupportedCommandTypes.class);
        //TODO goes against fuzz testing, and also limits to a very specific table existing...
        // There is a branch that can generate random transactions, so maybe look into that?
        PartialTxn txn = createPartialTxn(0);
        return rs -> {
            TxnId id = ids.next(rs);
            Timestamp executeAt = id;
            if (rs.nextBoolean())
                executeAt = ids.next(rs);
            SupportedCommandTypes targetType = supportedTypes.next(rs);
            switch (targetType)
            {
                case notWitnessed: return AccordTestUtils.Commands.notWitnessed(id, txn);
                case preaccepted: return AccordTestUtils.Commands.preaccepted(id, txn, executeAt);
                case committed: return AccordTestUtils.Commands.committed(id, txn, executeAt);
                default: throw new UnsupportedOperationException("Unexpected type: " + targetType);
            }
        };
    }

}
