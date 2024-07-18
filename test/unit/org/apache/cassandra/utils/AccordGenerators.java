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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import accord.local.Command;
import accord.local.RedundantBefore;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.KeyDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.AccordSplitter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordTestUtils;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.quicktheories.impl.JavaRandom;

import static org.apache.cassandra.service.accord.AccordTestUtils.TABLE_ID1;
import static org.apache.cassandra.service.accord.AccordTestUtils.createPartialTxn;

public class AccordGenerators
{
    private static final Gen<IPartitioner> PARTITIONER_GEN = fromQT(CassandraGenerators.PARTITIONER_GEN);

    private AccordGenerators()
    {
    }

    public static Gen<IPartitioner> partitioner()
    {
        return PARTITIONER_GEN;
    }

    private enum SupportedCommandTypes
    {notDefined, preaccepted, committed}

    public static Gen<Command> commands()
    {
        Gen<TxnId> ids = AccordGens.txnIds();
        //TODO switch to Status once all types are supported
        Gen<SupportedCommandTypes> supportedTypes = Gens.enums().all(SupportedCommandTypes.class);
        //TODO goes against fuzz testing, and also limits to a very specific table existing...
        // There is a branch that can generate random transactions, so maybe look into that?
        PartialTxn txn = createPartialTxn(0);
        FullRoute<?> route = txn.keys().toRoute(txn.keys().get(0).someIntersectingRoutingKey(null));

        return rs -> {
            TxnId id = ids.next(rs);
            Timestamp executeAt = id;
            if (rs.nextBoolean())
                executeAt = ids.next(rs);
            SupportedCommandTypes targetType = supportedTypes.next(rs);
            switch (targetType)
            {
                case notDefined:
                    return AccordTestUtils.Commands.notDefined(id, txn);
                case preaccepted:
                    return AccordTestUtils.Commands.preaccepted(id, txn, executeAt);
                case committed:
                    return AccordTestUtils.Commands.committed(id, txn, executeAt);
                default:
                    throw new UnsupportedOperationException("Unexpected type: " + targetType);
            }
        };
    }

    public static Gen<PartitionKey> keys()
    {
        return keys(fromQT(CassandraGenerators.TABLE_ID_GEN),
                    fromQT(CassandraGenerators.decoratedKeys()));
    }

    public static Gen<PartitionKey> keys(IPartitioner partitioner)
    {
        return keys(fromQT(CassandraGenerators.TABLE_ID_GEN),
                    fromQT(CassandraGenerators.decoratedKeys(ignore -> partitioner)));
    }

    public static Gen<PartitionKey> keys(Gen<TableId> tableIdGen, Gen<DecoratedKey> key)
    {
        return rs -> new PartitionKey(tableIdGen.next(rs), key.next(rs));
    }

    public static Gen<AccordRoutingKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<Token> tokenGen)
    {
        return routingKeyGen(tableIdGen, Gens.enums().all(AccordRoutingKey.RoutingKeyKind.class), tokenGen);
    }

    public static Gen<AccordRoutingKey> routingKeyGen(Gen<TableId> tableIdGen, Gen<AccordRoutingKey.RoutingKeyKind> kindGen, Gen<Token> tokenGen)
    {
        return rs -> {
            TableId tableId = tableIdGen.next(rs);
            AccordRoutingKey.RoutingKeyKind kind = kindGen.next(rs);
            switch (kind)
            {
                case TOKEN:
                    return new AccordRoutingKey.TokenKey(tableId, tokenGen.next(rs));
                case SENTINEL:
                    return rs.nextBoolean() ? AccordRoutingKey.SentinelKey.min(tableId) : AccordRoutingKey.SentinelKey.max(tableId);
                default:
                    throw new AssertionError("Unknown kind: " + kind);
            }
        };
    }

    public static Gen<Range> range()
    {
        return PARTITIONER_GEN.flatMap(partitioner -> range(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(CassandraGenerators.token(partitioner))));
    }

    public static Gen<Range> range(IPartitioner partitioner)
    {
        return range(fromQT(CassandraGenerators.TABLE_ID_GEN), fromQT(CassandraGenerators.token(partitioner)));
    }

    public static Gen<Range> range(Gen<TableId> tables, Gen<Token> tokenGen)
    {
        return rs -> {
            Gen<AccordRoutingKey> gen = routingKeyGen(Gens.constant(tables.next(rs)), tokenGen);
            AccordRoutingKey a = gen.next(rs);
            AccordRoutingKey b = gen.next(rs);
            while (a.equals(b))
                b = gen.next(rs);
            if (a.compareTo(b) < 0) return new TokenRange(a, b);
            else                    return new TokenRange(b, a);
        };
    }

    public static Gen<Ranges> ranges()
    {
        // javac couldn't pick the right constructor with HashSet::new, so had to create new lambda...
        return ranges(Gens.lists(fromQT(CassandraGenerators.TABLE_ID_GEN)).unique().ofSizeBetween(1, 10).map(l -> new HashSet<>(l)), PARTITIONER_GEN);
    }

    public static Gen<Ranges> ranges(Gen<Set<TableId>> tableIdGen, Gen<IPartitioner> partitionerGen)
    {
        return rs -> {
            Set<TableId> tables = tableIdGen.next(rs);
            IPartitioner partitioner = partitionerGen.next(rs);
            List<Range> ranges = new ArrayList<>();
            int numSplits = rs.nextInt(10, 100);
            TokenRange range = new TokenRange(AccordRoutingKey.SentinelKey.min(TABLE_ID1), AccordRoutingKey.SentinelKey.max(TABLE_ID1));
            AccordSplitter splitter = partitioner.accordSplitter().apply(Ranges.of(range));
            BigInteger size = splitter.sizeOf(range);
            BigInteger update = splitter.divide(size, numSplits);
            BigInteger offset = BigInteger.ZERO;
            while (offset.compareTo(size) < 0)
            {
                BigInteger end = offset.add(update);
                TokenRange r = (TokenRange) splitter.subRange(range, offset, end);
                for (TableId id : tables)
                {
                    ranges.add(r.withTable(id));
                }
                offset = end;
            }
            return Ranges.of(ranges.toArray(new Range[0]));
        };
    }

    public static Gen<Ranges> ranges(IPartitioner partitioner)
    {
        return ranges(Gens.lists(fromQT(CassandraGenerators.TABLE_ID_GEN)).unique().ofSizeBetween(1, 10).map(l -> new HashSet<>(l)), ignore -> partitioner);
    }

    public static Gen<Ranges> rangesArbitrary(IPartitioner partitioner)
    {
        Gen<Range> rangeGen = range(partitioner);
        Gen.IntGen sizeGen = Gens.ints().between(0, 10);
        return rs -> {
            int targetSize = sizeGen.nextInt(rs);
            List<Range> ranges = new ArrayList<>(targetSize);
            for (int i = 0; i < targetSize; i++)
                ranges.add(rangeGen.next(rs));
            return Ranges.of(ranges.toArray(Range[]::new));
        };
    }

    public static Gen<KeyDeps> keyDepsGen()
    {
        return AccordGens.keyDeps(AccordGenerators.keys());
    }

    public static Gen<KeyDeps> keyDepsGen(IPartitioner partitioner)
    {
        return AccordGens.keyDeps(AccordGenerators.keys(partitioner));
    }

    public static Gen<KeyDeps> directKeyDepsGen()
    {
        return AccordGens.directKeyDeps(AccordGenerators.keys());
    }

    public static Gen<KeyDeps> directKeyDepsGen(IPartitioner partitioner)
    {
        return AccordGens.directKeyDeps(AccordGenerators.keys(partitioner));
    }

    public static Gen<RangeDeps> rangeDepsGen()
    {
        return AccordGens.rangeDeps(AccordGenerators.range());
    }

    public static Gen<RangeDeps> rangeDepsGen(IPartitioner partitioner)
    {
        return AccordGens.rangeDeps(AccordGenerators.range(partitioner));
    }

    public static Gen<Deps> depsGen()
    {
        return AccordGens.deps(keyDepsGen(), rangeDepsGen(), directKeyDepsGen());
    }

    public static Gen<Deps> depsGen(IPartitioner partitioner)
    {
        return AccordGens.deps(keyDepsGen(partitioner), rangeDepsGen(partitioner), directKeyDepsGen(partitioner));
    }

    public static Gen<RedundantBefore.Entry> redundantBeforeEntry(IPartitioner partitioner)
    {
        return redundantBeforeEntry(Gens.bools().all(), range(partitioner), AccordGens.txnIds(Gens.pick(Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range));
    }

    public static Gen<RedundantBefore.Entry> redundantBeforeEntry(Gen<Boolean> emptyGen, Gen<Range> rangeGen, Gen<TxnId> txnIdGen)
    {
        return rs -> {
            Range range = rangeGen.next(rs);
            TxnId locallyAppliedOrInvalidatedBefore = emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs); // emptyable or range
            TxnId shardAppliedOrInvalidatedBefore = emptyGen.next(rs) ? TxnId.NONE : txnIdGen.next(rs); // emptyable or range
            TxnId bootstrappedAt = txnIdGen.next(rs);
            Timestamp staleUntilAtLeast = emptyGen.next(rs) ? null : txnIdGen.next(rs); // nullable

            long maxEpoch = Stream.of(locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast).filter(t -> t != null).mapToLong(Timestamp::epoch).max().getAsLong();
            long startEpoch = rs.nextLong(maxEpoch);
            long endEpoch = emptyGen.next(rs) ? Long.MAX_VALUE : 1 + rs.nextLong(startEpoch, Long.MAX_VALUE);
            return new RedundantBefore.Entry(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
        };
    }

    public static Gen<RedundantBefore> redundantBefore(IPartitioner partitioner)
    {
        Gen<Ranges> rangeGen = rangesArbitrary(partitioner);
        Gen<TxnId> txnIdGen = AccordGens.txnIds(Gens.pick(Txn.Kind.SyncPoint, Txn.Kind.ExclusiveSyncPoint), ignore -> Routable.Domain.Range);
        BiFunction<RandomSource, Range, RedundantBefore.Entry> entryGen = (rs, range) -> redundantBeforeEntry(Gens.bools().all(), i -> range, txnIdGen).next(rs);
        return AccordGens.redundantBefore(rangeGen, entryGen);
    }

    public static <T> Gen<T> fromQT(org.quicktheories.core.Gen<T> qt)
    {
        return rs -> {
            JavaRandom r = new JavaRandom(rs.asJdkRandom());
            return qt.generate(r);
        };
    }
}
