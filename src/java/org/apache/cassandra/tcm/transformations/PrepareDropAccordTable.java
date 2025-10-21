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
import java.util.Objects;

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.DropAccordTable;
import org.apache.cassandra.tcm.sequences.DropAccordTable.TableReference;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class PrepareDropAccordTable implements Transformation
{
    public static final Serializer serializer = new Serializer();

    public final TableReference tableRef;

    public PrepareDropAccordTable(TableReference tableRef)
    {
        this.tableRef = tableRef;
    }

    @Override
    public Kind kind()
    {
        return Kind.PREPARE_DROP_ACCORD_TABLE;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        TableMetadata metadata = prev.schema.getKeyspaces().getTableOrViewNullable(tableRef.id);
        if (metadata == null)
            return new Rejected(ExceptionCode.INVALID, "Table " + tableRef + " is not known");
        if (!metadata.requiresAccordSupport())
            return new Rejected(ExceptionCode.INVALID, "Table " + metadata + " is not an Accord table and should be dropped normally");
        if (metadata.params.pendingDrop)
            return new Rejected(ExceptionCode.INVALID, "Table " + metadata + " is in the process of being dropped");

        KeyspaceMetadata ks = prev.schema.getKeyspaceMetadata(metadata.keyspace);
        metadata = metadata.unbuild().params(metadata.params.unbuild().pendingDrop(true).build()).build();
        ks = ks.withSwapped(ks.tables.withSwapped(metadata));

        DropAccordTable operation = DropAccordTable.newSequence(tableRef, prev.nextEpoch());
        ClusterMetadata.Transformer proposed = prev.transformer()
                                                   .with(new DistributedSchema(prev.schema.getKeyspaces().withAddedOrUpdated(ks)))
                                                   .with(prev.inProgressSequences.with(tableRef, operation));
        return Transformation.success(proposed, LockedRanges.AffectedRanges.EMPTY);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PrepareDropAccordTable that = (PrepareDropAccordTable) o;
        return tableRef.equals(that.tableRef);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tableRef);
    }

    public static class Serializer implements AsymmetricMetadataSerializer<Transformation, PrepareDropAccordTable>
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            PrepareDropAccordTable plan = (PrepareDropAccordTable) t;
            TableReference.serializer.serialize(plan.tableRef, out, version);
        }

        @Override
        public PrepareDropAccordTable deserialize(DataInputPlus in, Version version) throws IOException
        {
            TableReference table = TableReference.serializer.deserialize(in, version);
            return new PrepareDropAccordTable(table);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            PrepareDropAccordTable plan = (PrepareDropAccordTable) t;
            return TableReference.serializer.serializedSize(plan.tableRef, version);
        }
    }
}
