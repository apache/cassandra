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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.SnapshotDetailsTabularData;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.TimestampType;
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
    private static final String CREATED_AT = "created_at";
    private static final String EXPIRES_AT = "expires_at";
    private static final String EPHEMERAL = "ephemeral";

    private final static DateFormat df;
    static
    {
        df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    SnapshotsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "snapshots")
                           .comment("available snapshots")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(ID, UTF8Type.instance)
                           .addClusteringColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME, UTF8Type.instance)
                           .addRegularColumn(TRUE_SIZE, UTF8Type.instance)
                           .addRegularColumn(SIZE_ON_DISK, UTF8Type.instance)
                           .addRegularColumn(CREATED_AT, TimestampType.instance)
                           .addRegularColumn(EXPIRES_AT, TimestampType.instance)
                           .addRegularColumn(EPHEMERAL, BooleanType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        // include snapshots with ttl and ephemeral
        Map<String, String> options = ImmutableMap.of("no_ttl", "false", "include_ephemeral", "true");
        Map<String, TabularData> snapshotDetails = StorageService.instance.getSnapshotDetails(options);

        for (Map.Entry<String, TabularData> snapshotDetailsEntry : snapshotDetails.entrySet())
        {
            String snapshotName = snapshotDetailsEntry.getKey();
            TabularDataSupport snapshotDetail = (TabularDataSupport) snapshotDetailsEntry.getValue();
            assert snapshotDetail.getTabularType() == SnapshotDetailsTabularData.TABULAR_TYPE;

            for (Object eachValue : snapshotDetail.keySet())
            {
                List<?> value = (List<?>) eachValue;
                assert value != null && value.size() == 8 : "snapshot detail must have 8 elements";
                Object[] rowValue = value.toArray();

                Date createdAt = parseTimestamp((String) rowValue[5]);
                Date expiresAt = parseTimestamp((String) rowValue[6]);

                result.row(snapshotName, rowValue[1], rowValue[2])
                      .column(TRUE_SIZE, rowValue[3])
                      .column(SIZE_ON_DISK, rowValue[4])
                      .column(CREATED_AT, createdAt)
                      .column(EXPIRES_AT, expiresAt)
                      .column(EPHEMERAL, Boolean.valueOf((String) rowValue[7]));
            }
        }

        return result;
    }

    public static Date parseTimestamp(String timestamp)
    {
        if (timestamp == null)
            return null;
        try
        {
            return df.parse(timestamp);
        }
        catch (Exception ex)
        {
            throw new IllegalStateException(String.format("Unable to parse timestamp %s", timestamp), ex);
        }
    }
}