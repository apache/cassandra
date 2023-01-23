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

package org.apache.cassandra.tcm.transformations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ConsensusTableMigrationState;
import org.apache.cassandra.service.ConsensusTableMigrationState.ConsensusMigrationRepairType;
import org.apache.cassandra.service.ConsensusTableMigrationState.ConsensusMigrationTarget;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.cassandra.dht.Range.intersects;
import static org.apache.cassandra.dht.Range.normalize;
import static org.apache.cassandra.service.ConsensusTableMigrationState.MigrationStateSnapshot;
import static org.apache.cassandra.service.ConsensusTableMigrationState.TableMigrationState;


public class MaybeFinishConsensusMigrationForTableAndRange implements Transformation
{
    public static Serializer serializer = new Serializer();

    @Nonnull
    public final String keyspace;

    @Nonnull
    public final String cf;

    @Nonnull
    public final Collection<Range<Token>> repairedRanges;

    @Nonnull
    public final Epoch minEpoch;

    @Nonnull
    public final ConsensusMigrationRepairType repairType;

    public MaybeFinishConsensusMigrationForTableAndRange(@Nonnull String keyspace,
                                                         @Nonnull String cf,
                                                         @Nonnull List<Range<Token>> repairedRanges,
                                                         @Nonnull Epoch minEpoch,
                                                         @Nonnull ConsensusMigrationRepairType repairType)
    {
        checkNotNull(keyspace, "keyspace should not be null");
        checkNotNull(cf, "cf should not be null");
        checkNotNull(repairedRanges, "repairedRanges should not be null");
        checkArgument(!repairedRanges.isEmpty(), "repairedRanges should not be empty");
        checkNotNull(minEpoch, "minEpoch should not be null");
        checkArgument(minEpoch.isAfter(Epoch.EMPTY), "minEpoch should not be empty");
        checkNotNull(repairType, "repairType is null");
        checkArgument(repairType != ConsensusMigrationRepairType.ineligible, "Shouldn't attempt to finish migration with ineligible repair");
        this.keyspace = keyspace;
        this.cf = cf;
        this.repairedRanges = repairedRanges;
        this.minEpoch = minEpoch;
        this.repairType = repairType;
    }

    public Kind kind()
    {
        return Kind.MAYBE_FINISH_CONSENSUS_MIGRATION_FOR_TABLE_AND_RANGE;
    }

    public Result execute(@Nonnull ClusterMetadata metadata)
    {
        checkNotNull(metadata, "clusterMetadata should not be null");
        String ksAndCF = keyspace + "." + cf;
        TableMetadata tbm = Schema.instance.getTableMetadata(keyspace, cf);
        if (tbm == null)
            return new Rejected(format("Table %s is not currently performing consensus migration", ksAndCF));

        MigrationStateSnapshot migrationStateSnapshot = metadata.migrationStateSnapshot;
        ConsensusTableMigrationState.TableMigrationState tms = migrationStateSnapshot.tableStates.get(tbm.id);
        if (tms == null)
            return new Rejected(format("Table %s is not currently performing consensus migration", ksAndCF));

        if (tms.targetProtocol == ConsensusMigrationTarget.accord && repairType != ConsensusMigrationRepairType.paxos)
            return new Rejected(format("Table %s is not currently performing consensus migration to Accord and the repair was a Paxos repair", ksAndCF));

        if (tms.targetProtocol == ConsensusMigrationTarget.paxos && repairType != ConsensusMigrationRepairType.accord)
            return new Rejected(format("Table %s is not currently performing consensus migration to Paxos and the repair was an Accord repair", ksAndCF));

        List<Range<Token>> normalizedRepairedRanges = normalize(repairedRanges);

        // Bail out if repair doesn't actually intersect with any migrating ranges
        if (!intersects(tms.migratingRanges, normalizedRepairedRanges))
            return new Rejected(format("Table %s is migrating ranges %s, which doesn't include repaired ranges %s", ksAndCF, tms.migratingRanges, normalizedRepairedRanges));

        TableMigrationState newTableMigrationState = tms.withRangesRepairedAtEpoch(normalizedRepairedRanges, minEpoch);

        return new Success(metadata.transformer().withConsensusTableMigrationStates(ImmutableMap.of(newTableMigrationState.tableId, newTableMigrationState)).build(), LockedRanges.AffectedRanges.EMPTY);
    }

    static class Serializer implements AsymmetricMetadataSerializer<Transformation, MaybeFinishConsensusMigrationForTableAndRange>
    {
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            MaybeFinishConsensusMigrationForTableAndRange v = (MaybeFinishConsensusMigrationForTableAndRange)t;
            out.writeUTF(v.keyspace);
            out.writeUTF(v.cf);
            out.writeInt(v.repairedRanges.size());
            for (Range<Token> range : v.repairedRanges)
            {
                Token.metadataSerializer.serialize(range.left, out, version);
                Token.metadataSerializer.serialize(range.right, out, version);
            }
            Epoch.serializer.serialize(v.minEpoch, out, version);
            out.write(v.repairType.value);
        }

        public MaybeFinishConsensusMigrationForTableAndRange deserialize(DataInputPlus in, Version version) throws IOException
        {
            String keyspace = in.readUTF();
            String cf = in.readUTF();
            int numRepairedRanges = in.readInt();
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();
            List<Range<Token>> repairedRanges = new ArrayList<>(numRepairedRanges);
            for (int i = 0; i < numRepairedRanges; i++)
            {
                repairedRanges.add(new Range<>(Token.metadataSerializer.deserialize(in, partitioner, version),
                                               Token.metadataSerializer.deserialize(in, partitioner, version)));
            }
            Epoch minEpoch = Epoch.serializer.deserialize(in, version);
            ConsensusMigrationRepairType repairType = ConsensusMigrationRepairType.fromValue(in.readByte());
            return new MaybeFinishConsensusMigrationForTableAndRange(keyspace, cf, repairedRanges, minEpoch, repairType);
        }

        public long serializedSize(Transformation t, Version version)
        {
            MaybeFinishConsensusMigrationForTableAndRange v = (MaybeFinishConsensusMigrationForTableAndRange)t;
            long size = TypeSizes.sizeof(v.keyspace)
                + TypeSizes.sizeof(v.cf)
                + TypeSizes.sizeof(v.repairedRanges.size())
                + Epoch.serializer.serializedSize(v.minEpoch)
                + TypeSizes.sizeof(v.repairType.value);
            for (Range<Token> range : v.repairedRanges)
            {
                size += Token.metadataSerializer.serializedSize(range.left, version);
                size += Token.metadataSerializer.serializedSize(range.right, version);
            }
            return size;
        }
    }
}