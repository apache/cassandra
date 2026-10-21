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

import java.io.IOException;

import org.apache.cassandra.db.AbstractCompactionController;
import org.apache.cassandra.utils.TimeUUID;

class CursorCompactionPipeline extends AbstractCompactionPipeline {
    final CursorCompactor cursorCompactor;

    CursorCompactionPipeline(CompactionTask task, OperationType type, AbstractCompactionStrategy.ScannerList scanners, AbstractCompactionController controller, long nowInSec, TimeUUID compactionId) {
        super(task);
        cursorCompactor = new CursorCompactor(type, scanners.scanners, controller, nowInSec, compactionId);
    }

    @Override
    CompactionInfo.Holder delegate() {
        return cursorCompactor;
    }

    @Override
    boolean processNextPartitionKey() throws IOException {
        if (cursorCompactor.writeNextPartition(writer)) {
            totalKeysWritten++;
            cursorCompactor.setTargetDirectory(writer.getSStableDirectoryPath());
            return true;
        }
        return false;
    }

    @Override
    public long[] getMergedRowCounts() {
        return cursorCompactor.getMergedRowsCounts();
    }

    @Override
    public long getTotalSourceCQLRows() {
        return cursorCompactor.getTotalSourceCQLRows();
    }

    @Override
    public long getTotalBytesScanned() {
        return cursorCompactor.getTotalBytesScanned();
    }

    @Override
    public void close() throws IOException {
        cursorCompactor.close();
    }
}
