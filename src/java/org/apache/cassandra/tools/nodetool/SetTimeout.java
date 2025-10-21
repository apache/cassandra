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


import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.nodetool.layout.CassandraUsage;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.cassandra.tools.nodetool.CommandUtils.concatArgs;

@Command(name = "settimeout", description = "Set the specified timeout in ms, or 0 to disable timeout")
public class SetTimeout extends AbstractCommand
{
    @CassandraUsage(usage = "<timeout_type> <timeout_in_ms>", description = "Timeout type followed by value in ms " +
                                                                            "(0 disables socket streaming timeout). Type should be one of (" + GetTimeout.TIMEOUT_TYPES + ")")
    private List<String> args = new ArrayList<>();

    @Parameters(paramLabel = "timeout_type", description = "Timeout type", index = "0", arity = "0..1")
    private String timeoutType;

    @Parameters(paramLabel = "timeout_in_ms", description = "Timeout in ms", index = "1", arity = "0..1")
    private String timeoutInMs;

    @Override
    public void execute(NodeProbe probe)
    {
        args = concatArgs(timeoutType, timeoutInMs);

        checkArgument(args.size() == 2, "Timeout type followed by value in ms (0 disables socket streaming timeout)." +
                " Type should be one of (" + GetTimeout.TIMEOUT_TYPES + ")");

        try
        {
            String type = args.get(0);
            long timeout = Long.parseLong(args.get(1));
            probe.setTimeout(type, timeout);
        } catch (Exception e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
