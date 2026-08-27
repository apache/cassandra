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
import java.util.Collection;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.replication.migration.KeyspaceMigrationInfo;
import org.apache.cassandra.service.replication.migration.MutationTrackingMigrationState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;
import static org.apache.cassandra.tcm.ClusterMetadata.Transformer;
import static org.apache.cassandra.utils.CollectionSerializers.deserializeList;
import static org.apache.cassandra.utils.CollectionSerializers.serializeCollection;
import static org.apache.cassandra.utils.CollectionSerializers.serializedCollectionSize;

/**
 * Transformation to mark ranges as migrated for a keyspace.
 *
 * Called by repair coordinator callback to report completed ranges to TCM.
 * Subtracts completed ranges from pendingRangesPerTable and automatically removes
 * keyspace from migration state when all tables are fully repaired (migration complete).
 */
public class AdvanceMutationTrackingMigration implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(AdvanceMutationTrackingMigration.class);
    public static final Serializer serializer = new Serializer();

    @Nonnull
    public final String keyspace;

    @Nonnull
    public final TableId tableId;

    @Nonnull
    public final Collection<Range<Token>> repairedRanges;

    public AdvanceMutationTrackingMigration(@Nonnull String keyspace,
                                            @Nonnull TableId tableId,
                                            @Nonnull Collection<Range<Token>> repairedRanges)
    {
        checkNotNull(keyspace, "keyspace should not be null");
        checkNotNull(tableId, "tableId should not be null");
        checkArgument(repairedRanges != null && !repairedRanges.isEmpty(),
                      "repairedRanges should not be null/empty");
        this.keyspace = keyspace;
        this.tableId = tableId;
        this.repairedRanges = repairedRanges;
    }

    @Override
    public Kind kind()
    {
        return Kind.ADVANCE_MUTATION_TRACKING_MIGRATION;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        KeyspaceMigrationInfo ksInfo = prev.mutationTrackingMigrationState.getKeyspaceInfo(keyspace);

        if (ksInfo == null)
            return new Rejected(INVALID, String.format("Keyspace %s is not migrating", keyspace));

        Transformer transformer = prev.transformer();

        // Subtract repaired ranges from table's pending set, auto-removes keyspace if all tables complete
        MutationTrackingMigrationState newState = prev.mutationTrackingMigrationState
            .withRangesRepairedForTable(keyspace, tableId, repairedRanges, transformer.epoch());

        if (newState == prev.mutationTrackingMigrationState)
        {
            return new Rejected(INVALID, String.format("Keyspace %s table %s has no pending ranges intersecting %s",
                                                       keyspace, tableId, repairedRanges));
        }

        return Transformation.success(
            transformer.with(newState),
            LockedRanges.AffectedRanges.EMPTY);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, AdvanceMutationTrackingMigration>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdvanceMutationTrackingMigration v = (AdvanceMutationTrackingMigration) t;
            out.writeUTF(v.keyspace);
            v.tableId.serializeCompact(out);
            serializeCollection(v.repairedRanges, out, version, Range.serializer);
        }

        @Override
        public AdvanceMutationTrackingMigration deserialize(DataInputPlus in, Version version) throws IOException
        {
            String keyspace = in.readUTF();
            TableId tableId = TableId.deserializeCompact(in);
            Collection<Range<Token>> repairedRanges = deserializeList(in, version, Range.serializer);
            return new AdvanceMutationTrackingMigration(keyspace, tableId, repairedRanges);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            AdvanceMutationTrackingMigration v = (AdvanceMutationTrackingMigration) t;
            return TypeSizes.sizeof(v.keyspace)
                   + v.tableId.serializedCompactSize()
                   + serializedCollectionSize(v.repairedRanges, version, Range.serializer);
        }
    }
}
