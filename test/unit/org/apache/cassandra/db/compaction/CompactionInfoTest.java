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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.repair.AbstractPendingAntiCompactionTest;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class CompactionInfoTest extends AbstractPendingAntiCompactionTest
{
    @Test
    public void testCompactionInfoToStringContainsTaskId()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        TimeUUID expectedTaskId = nextTimeUUID();
        CompactionInfo compactionInfo = new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, 0, 1000, expectedTaskId, new ArrayList<>());
        Assertions.assertThat(compactionInfo.toString())
                  .contains(expectedTaskId.toString());
    }

    @Test
    public void testCompactionInfoToStringFormat()
    {
        UUID tableId = UUID.randomUUID();
        TimeUUID taskId = nextTimeUUID();
        ColumnFamilyStore cfs = MockSchema.newCFS(builder -> builder.id(TableId.fromUUID(tableId)));
        CompactionInfo compactionInfo = new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, 0, 1000, taskId, new ArrayList<>());
        Assertions.assertThat(compactionInfo.toString())
                  .isEqualTo("Compaction(%s, 0 / 1000 bytes)@%s(mockks, mockcf1)", taskId, tableId);
    }
}
