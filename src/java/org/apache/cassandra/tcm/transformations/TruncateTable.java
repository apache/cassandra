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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.extensions.TableTruncationValue;
import org.apache.cassandra.tcm.extensions.TableTruncationValue.TableTruncation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static java.lang.String.format;
import static org.apache.cassandra.exceptions.ExceptionCode.INVALID;

public class TruncateTable implements Transformation
{
    private static final Logger logger = LoggerFactory.getLogger(TruncateTable.class);

    // how to identify and obtain a serializer for instances of this Transformation
    public static final String NAME = TruncateTable.class.getName();

    public static ExtensionKey<TableTruncation, TableTruncationValue> METADATA_KEY = new ExtensionKey<>(NAME, TableTruncationValue.class);

    private final String keyspace;
    private final String table;
    private final long timestamp;

    public TruncateTable(String keyspace, String table, long timestamp)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.timestamp = timestamp;
    }

    @Override
    public Kind kind()
    {
        return Kind.TRUNCATE_TABLE;
    }

    @Override
    public String toString()
    {
        return "TruncateTable{" +
               "keyspace='" + keyspace + '\'' +
               ", table='" + table + '\'' +
               ", timestamp=" + timestamp +
               '}';
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        TableTruncationValue value = TableTruncationValue.create(new TableTruncation(keyspace, table, timestamp));
        return Transformation.success(prev.transformer()
                                          .with(new ExtensionKey<>(value.getValue().toString(), TableTruncationValue.class), value),
                                      LockedRanges.AffectedRanges.EMPTY);
    }

    private Result validate(ClusterMetadata metadata)
    {
        KeyspaceMetadata ksm = metadata.schema.getKeyspaceMetadata(keyspace);

        if (ksm == null)
            return new Rejected(INVALID, "Keyspace " + keyspace + " does not exist");

        TableMetadata tmd = ksm.getTableOrViewNullable(table);

        if (tmd == null)
            return new Rejected(INVALID, format("Table %s.%s does not exist", keyspace, table));

        if (tmd.isView())
            return new Rejected(INVALID, format("Cannot TRUNCATE materialized view %s.%s directly; must truncate base table instead", keyspace, table));

        // this should not happen but just to be sure ...
        if (tmd.isVirtual())
            return new Rejected(INVALID, format("Cannot TRUNCATE virtual table %s.%s via TCM", keyspace, table));

        return Transformation.success(metadata.transformer(), LockedRanges.AffectedRanges.EMPTY);
    }

    public static final AsymmetricMetadataSerializer<Transformation, TruncateTable> serializer = new AsymmetricMetadataSerializer<>()
    {
        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            assert t instanceof TruncateTable;
            TruncateTable truncate = (TruncateTable) t;
            out.writeUTF(truncate.keyspace);
            out.writeUTF(truncate.table);
            out.writeLong(truncate.timestamp);
        }

        @Override
        public TruncateTable deserialize(DataInputPlus in, Version version) throws IOException
        {
            String ks = in.readUTF();
            String tb = in.readUTF();
            long timetstamp = in.readLong();
            return new TruncateTable(ks, tb, timetstamp);
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            assert t instanceof TruncateTable;
            TruncateTable truncate = (TruncateTable) t;
            return (long) TypeSizes.sizeof(truncate.keyspace) + TypeSizes.sizeof(truncate.table) + TypeSizes.sizeof(truncate.timestamp);
        }
    };
}
