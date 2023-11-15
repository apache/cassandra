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

package org.apache.cassandra.db.commitlog;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;

public class CommitLogSegmentManagerStandard extends AbstractCommitLogSegmentManager
{
    public CommitLogSegmentManagerStandard(final CommitLog commitLog, String storageDirectory)
    {
        super(commitLog, storageDirectory);
    }

    public void discard(CommitLogSegment segment, boolean delete)
    {
        segment.close();
        if (delete)
            FileUtils.deleteWithConfirm(segment.logFile);
        addSize(-segment.onDiskSize());
    }

    /**
     * Reserve space in the current segment for the provided mutation or, if there isn't space available,
     * create a new segment. allocate() is blocking until allocation succeeds as it waits on a signal in advanceAllocatingFrom
     *
     * @param mutation mutation to allocate space for
     * @param size total size of mutation (overhead + serialized size)
     * @return the provided Allocation object
     */
    public CommitLogSegment.Allocation allocate(Mutation mutation, int size)
    {
        CommitLogSegment segment = allocatingFrom();

        CommitLogSegment.Allocation alloc;
        while ( null == (alloc = segment.allocate(mutation, size)) )
        {
            // failed to allocate, so move to a new segment with enough room
            advanceAllocatingFrom(segment);
            segment = allocatingFrom();
        }

        return alloc;
    }

    @Override
    public CommitLogSegment createSegment()
    {
        CommitLogSegment segment = super.createSegment();
        segment.writeLogHeader();
        return segment;
    }
}
