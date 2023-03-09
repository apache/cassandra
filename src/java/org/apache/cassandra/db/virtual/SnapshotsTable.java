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

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.SnapshotDetailsTabularData;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

public class SnapshotsTable extends AbstractVirtualTable
{
    private static final String ID = "id";
    private static final String KEYSPACE_NAME = "keyspace_name";
    private static final String TABLE_NAME = "table_name";
    private static final String TRUE_SIZE = "true_size";
    private static final String SIZE_ON_DISK = "size_on_disk";
    private static final String CREATE_TIME = "created_at";
    private static final String EXPIRATION_TIME = "expires_at";
    private static final String EPHEMERAL = "ephemeral";

    SnapshotsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "snapshots")
                           .comment("available snapshots")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(ID, UTF8Type.instance)
                           .addClusteringColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(TRUE_SIZE, LongType.instance)
                           .addRegularColumn(SIZE_ON_DISK, LongType.instance)
                           .addRegularColumn(CREATE_TIME, UTF8Type.instance)
                           .addRegularColumn(EXPIRATION_TIME, UTF8Type.instance)
                           .addRegularColumn(EPHEMERAL, BooleanType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        // include snapshots with ttl and ephemeral
        Map<String, TabularData> snapshotDetails = StorageService.instance.getSnapshotDetails(ImmutableMap.of("no_ttl", "false", "include_ephemeral", "true"));
        for (Map.Entry<String, TabularData> entry : snapshotDetails.entrySet())
        {
            String snapshotName = entry.getKey();
            TabularDataSupport snapInfo = (TabularDataSupport) entry.getValue();
            assert snapInfo.getTabularType() == SnapshotDetailsTabularData.TABULAR_TYPE;
            Set<?> values = entry.getValue().keySet();
            for (Object eachValue : values)
            {
                final List<?> value = (List<?>) eachValue;
                assert value != null && value.size() == 8 : "snapshot info must have 8 elements";
                String[] rowValue = value.toArray(new String[value.size()]);
                result.row(snapshotName, rowValue[1], rowValue[2])
                      .column(TRUE_SIZE, rowValue[3])
                      .column(SIZE_ON_DISK, rowValue[4])
                      .column(CREATE_TIME, rowValue[5])
                      .column(EXPIRATION_TIME, rowValue[6])
                      .column(EPHEMERAL, Boolean.valueOf(rowValue[7]));
            }
        }

        return result;
    }
}