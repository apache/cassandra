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

package org.apache.cassandra.db.virtual;

import java.util.Date;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.snapshot.TableSnapshot;

public class SnapshotsTable extends AbstractVirtualTable
{
    private static final String NAME = "name";
    private static final String KEYSPACE_NAME = "keyspace_name";
    private static final String TABLE_NAME = "table_name";
    private static final String TRUE_SIZE = "true_size";
    private static final String SIZE_ON_DISK = "size_on_disk";
    private static final String CREATED_AT = "created_at";
    private static final String EXPIRES_AT = "expires_at";
    private static final String EPHEMERAL = "ephemeral";

    SnapshotsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "snapshots")
                           .comment("available snapshots")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(NAME, UTF8Type.instance)
                           .addClusteringColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(TRUE_SIZE, LongType.instance)
                           .addRegularColumn(SIZE_ON_DISK, LongType.instance)
                           .addRegularColumn(CREATED_AT, TimestampType.instance)
                           .addRegularColumn(EXPIRES_AT, TimestampType.instance)
                           .addRegularColumn(EPHEMERAL, BooleanType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (TableSnapshot tableSnapshot : StorageService.instance.snapshotManager.loadSnapshots())
        {
            SimpleDataSet row = result.row(tableSnapshot.getTag(),
                                           tableSnapshot.getKeyspaceName(),
                                           tableSnapshot.getTableName())
                                      .column(TRUE_SIZE, tableSnapshot.computeTrueSizeBytes())
                                      .column(SIZE_ON_DISK, tableSnapshot.computeSizeOnDiskBytes())
                                      .column(CREATED_AT, new Date(tableSnapshot.getCreatedAt().toEpochMilli()));

            if (tableSnapshot.isExpiring())
                row.column(EXPIRES_AT, new Date(tableSnapshot.getExpiresAt().toEpochMilli()));

            row.column(EPHEMERAL, tableSnapshot.isEphemeral());
        }

        return result;
    }
}
