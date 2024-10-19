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

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "disablediagnosticlog", description = "Disable the diagnostic log")
public class DisableDiagnosticLog extends NodeTool.NodeToolCmd
{
    @Option(title = "keep_in_memory_log",
    name = { "-k", "--keep-in-memory-log" },
    description = "Keep in-memory diagnostic logger active, disable only persistent logger.")
    private boolean keepInMemoryLog = false;

    @Option(title = "clean_events",
    name = { "-c", "--clean"},
    description = "Clean in-memory event stores. This will also reset last event id's.")
    private boolean clean = false;

    @Override
    protected void execute(NodeProbe probe)
    {
        if (!keepInMemoryLog)
            probe.disableDiagnosticLog(clean);
        else
            probe.disablePersistentDiagnosticLog();
    }
}
