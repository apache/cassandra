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

package org.apache.cassandra.tcm.migration;

import org.apache.cassandra.tcm.Commit;
import org.apache.cassandra.tcm.Processor;
import org.apache.cassandra.tcm.Retry;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.log.LogState;

public class GossipProcessor implements Processor
{
    @Override
    public Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown, Retry.Deadline retryPolicy)
    {
        throw new IllegalStateException("Can't commit transformations when running in gossip mode. Enable the ClusterMetadataService with `nodetool cms initialize`.");
    }

    @Override
    public ClusterMetadata fetchLogAndWait(Epoch waitFor, Retry.Deadline retryPolicy)
    {
        return ClusterMetadata.current();
    }

    @Override
    public LogState getLocalState(Epoch start, Epoch end, boolean includeSnapshot, Retry.Deadline retryPolicy)
    {
        throw new IllegalStateException("Can't reconstruct log state when running in gossip mode. Enable the ClusterMetadataService with `nodetool addtocms`.");
    }

    @Override
    public LogState getLogState(Epoch start, Epoch end, boolean includeSnapshot, Retry.Deadline retryPolicy)
    {
        throw new IllegalStateException("Can't reconstruct log state when running in gossip mode. Enable the ClusterMetadataService with `nodetool addtocms`.");
    }
}
