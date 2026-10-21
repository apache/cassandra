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

class IteratorCompactionPipeline extends AbstractCompactionPipeline {
    final CompactionIterator ci;
    final AbstractCompactionStrategy.ScannerList scanners;

    IteratorCompactionPipeline(CompactionTask task, OperationType type, AbstractCompactionStrategy.ScannerList scanners, AbstractCompactionController controller, long nowInSec, TimeUUID compactionId) {
        super(task);
        this.scanners = scanners;
        ci = new CompactionIterator(type, this.scanners.scanners, controller, nowInSec, compactionId);
    }

    @Override
    CompactionInfo.Holder delegate() {
        return ci;
    }

    @Override
    boolean processNextPartitionKey() throws IOException {
        if (ci.hasNext()) {
            if (writer.append(ci.next()))
                totalKeysWritten++;
            ci.setTargetDirectory(writer.getSStableDirectoryPath());
            return true;
        }
        return false;
    }

    @Override
    public long[] getMergedRowCounts() {
        return ci.getMergedRowCounts();
    }

    @Override
    public long getTotalSourceCQLRows() {
        return ci.getTotalSourceCQLRows();
    }

    @Override
    public long getTotalBytesScanned() {
        return scanners.getTotalBytesScanned();
    }

    @Override
    public void close() throws IOException {
        ci.close();
    }
}
