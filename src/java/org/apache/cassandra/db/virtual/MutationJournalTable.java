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

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.journal.ActiveSegment;
import org.apache.cassandra.journal.Segment;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.schema.TableMetadata;

final class MutationJournalTable extends AbstractVirtualTable
{
    private static final String SEGMENT_ID = "segment_id";
    private static final String IS_ACTIVE = "is_active";
    private static final String SIZE_BYTES = "size_bytes";
    private static final String RECORDS_COUNT = "records_count";
    private static final String WRITTEN_TO = "written_to";
    private static final String FSYNCED_TO = "fsynced_to";
    private static final String NEEDS_REPLAY = "needs_replay";
    private static final String FILE_PATH = "file_path";

    MutationJournalTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "mutation_journal")
                           .comment("mutation journal segments and their contents")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(LongType.instance))
                           .addPartitionKeyColumn(SEGMENT_ID, LongType.instance)
                           .addRegularColumn(IS_ACTIVE, BooleanType.instance)
                           .addRegularColumn(SIZE_BYTES, LongType.instance)
                           .addRegularColumn(RECORDS_COUNT, Int32Type.instance)
                           .addRegularColumn(WRITTEN_TO, Int32Type.instance)
                           .addRegularColumn(FSYNCED_TO, Int32Type.instance)
                           .addRegularColumn(NEEDS_REPLAY, BooleanType.instance)
                           .addRegularColumn(FILE_PATH, UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

         for (Segment<ShortMutationId, Mutation> segment : MutationJournal.instance.getSegments())
         {
             result.row(segment.id())
                   .column(IS_ACTIVE, segment instanceof ActiveSegment)
                   .column(SIZE_BYTES, segment.segmentSizeOnDisk())
                   .column(RECORDS_COUNT, segment.metadata().totalCount())
                   .column(WRITTEN_TO, segment.writtenTo())
                   .column(FSYNCED_TO, segment.fsyncedTo())
                   .column(NEEDS_REPLAY, segment.metadata().needsReplay())
                   .column(FILE_PATH, segment.filePath());
         }

        return result;
    }
}
