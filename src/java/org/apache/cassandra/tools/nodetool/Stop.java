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
package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "stop", description = "Stop compaction")
public class Stop extends NodeToolCmd
{
    @Arguments(title = "compaction_type",
              usage = "<compaction type>",
              description = "Supported types are COMPACTION, VALIDATION, CLEANUP, SCRUB, UPGRADE_SSTABLES, INDEX_BUILD, TOMBSTONE_COMPACTION, ANTICOMPACTION, VERIFY, VIEW_BUILD, INDEX_SUMMARY, RELOCATE, GARBAGE_COLLECT",
              required = false)
    private OperationType compactionType = OperationType.UNKNOWN;

    @Option(title = "compactionId",
           name = {"-id", "--compaction-id"},
           description = "Use -id to stop a compaction by the specified id. Ids can be found in the transaction log files whose name starts with compaction_, located in the table transactions folder.",
           required = false)
    private String compactionId = "";

    @Override
    public void execute(NodeProbe probe)
    {
        if (!compactionId.isEmpty())
            probe.stopById(compactionId);
        else
            probe.stop(compactionType.name());
    }
}
