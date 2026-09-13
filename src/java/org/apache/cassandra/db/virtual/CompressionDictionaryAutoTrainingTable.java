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

import org.apache.cassandra.db.compression.CompressionDictionaryAutoTrainingHistory;
import org.apache.cassandra.db.compression.CompressionDictionaryAutoTrainingHistory.Entry;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Exposes the auto-training adoption decisions this node has made, as recorded by
 * {@link CompressionDictionaryAutoTrainingHistory}: when a candidate dictionary was trained, for which table,
 * how it compared against the dictionary already in use, and whether it was adopted.
 * <p>
 * Node-local and held in memory only, like the other {@code system_views} tables: a restart clears it, and only
 * the node that ran the training has rows for it.
 */
final class CompressionDictionaryAutoTrainingTable extends AbstractVirtualTable
{
    static final String TABLE_NAME = "compression_dictionary_auto_training";
    private static final String TABLE_COMMENT = "Auto-training decisions for compression dictionaries on this node";

    static final String KEYSPACE_NAME = "keyspace_name";
    static final String TABLE_NAME_COLUMN = "table_name";
    static final String TRAINED_AT = "trained_at";
    static final String NODE = "node";
    static final String KIND = "kind";
    static final String BASELINE_RATIO = "baseline_ratio";
    static final String CANDIDATE_RATIO = "candidate_ratio";
    static final String IMPROVEMENT = "improvement";
    static final String THRESHOLD = "threshold";
    static final String PROMOTED = "promoted";

    CompressionDictionaryAutoTrainingTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, TABLE_NAME)
                           .comment(TABLE_COMMENT)
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UTF8Type.instance))
                           .addPartitionKeyColumn(KEYSPACE_NAME, UTF8Type.instance)
                           .addClusteringColumn(TABLE_NAME_COLUMN, UTF8Type.instance)
                           // reversed so the most recent training for a table comes first
                           .addClusteringColumn(TRAINED_AT, ReversedType.getInstance(TimestampType.instance))
                           .addRegularColumn(NODE, InetAddressType.instance)
                           .addRegularColumn(KIND, UTF8Type.instance)
                           .addRegularColumn(BASELINE_RATIO, DoubleType.instance)
                           .addRegularColumn(CANDIDATE_RATIO, DoubleType.instance)
                           .addRegularColumn(IMPROVEMENT, DoubleType.instance)
                           .addRegularColumn(THRESHOLD, DoubleType.instance)
                           .addRegularColumn(PROMOTED, BooleanType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Entry entry : CompressionDictionaryAutoTrainingHistory.instance.entries())
        {
            result.row(entry.keyspaceName, entry.tableName, new Date(entry.timestampMillis))
                  .column(NODE, entry.node.getAddress())
                  .column(KIND, entry.kind == null ? null : entry.kind.name())
                  .column(BASELINE_RATIO, entry.baselineRatio)
                  .column(CANDIDATE_RATIO, entry.candidateRatio)
                  .column(IMPROVEMENT, entry.improvement)
                  .column(THRESHOLD, entry.threshold)
                  .column(PROMOTED, entry.promoted);
        }

        return result;
    }
}
