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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.CloseableIterator;

public abstract class AbstractCompactionIterable extends CompactionInfo.Holder implements Iterable<AbstractCompactedRow>
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractCompactionIterable.class);

    protected final OperationType type;
    protected final CompactionController controller;
    protected final long totalBytes;
    protected volatile long bytesRead;
    protected final List<ICompactionScanner> scanners;

    public AbstractCompactionIterable(CompactionController controller, OperationType type, List<ICompactionScanner> scanners)
    {
        this.controller = controller;
        this.type = type;
        this.scanners = scanners;
        this.bytesRead = 0;

        long bytes = 0;
        for (ICompactionScanner scanner : scanners)
            bytes += scanner.getLengthInBytes();
        this.totalBytes = bytes;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(type, bytesRead, totalBytes);
    }

    public abstract CloseableIterator<AbstractCompactedRow> iterator();
}
