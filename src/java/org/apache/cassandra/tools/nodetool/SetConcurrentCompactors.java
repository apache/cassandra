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

import static com.google.common.base.Preconditions.checkArgument;
import io.airlift.command.Arguments;
import io.airlift.command.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "setconcurrentcompactors", description = "Set number of concurrent compactors in the system.")
public class SetConcurrentCompactors extends NodeTool.NodeToolCmd
{
    @Arguments(title = "concurrent_compactors", usage = "<value>", description = "Number of concurrent compactors, greater than 0.", required = true)
    private Integer concurrentCompactors = null;

    protected void execute(NodeProbe probe)
    {
        checkArgument(concurrentCompactors > 0, "concurrent_compactors should be great than 0.");
        probe.setConcurrentCompactors(concurrentCompactors);
    }
}
