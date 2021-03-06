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

package org.apache.cassandra.distributed.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.impl.Instance;
import org.apache.cassandra.distributed.shared.NodeToolResultWithOutput;
import org.apache.cassandra.tools.Output;

// Prefer to use the NodeToolResult that includes output since version 0.0.5
// CASSANDRA-16057
public final class NodetoolUtils
{
    private NodetoolUtils()
    {

    }

    public static NodeToolResultWithOutput nodetool(IInvokableInstance inst, String... args)
    {
        return nodetool(inst, true, args);
    }

    public static NodeToolResultWithOutput nodetool(IInvokableInstance inst, boolean withNotifications, String... args)
    {
        return inst.callOnInstance(() -> {
            PrintStream originalSysOut = System.out;
            PrintStream originalSysErr = System.err;
            originalSysOut.flush();
            originalSysErr.flush();
            ByteArrayOutputStream toolOut = new ByteArrayOutputStream();
            ByteArrayOutputStream toolErr = new ByteArrayOutputStream();

            try (PrintStream newOut = new PrintStream(toolOut);
                 PrintStream newErr = new PrintStream(toolErr))
            {
                System.setOut(newOut);
                System.setErr(newErr);
                Instance.DTestNodeTool nodetool = new Instance.DTestNodeTool(withNotifications, new Output(newOut, newErr));
                int rc = nodetool.execute(args);
                NodeToolResult result = new NodeToolResult(args, rc, nodetool.getNotifications(), nodetool.getLatestError());
                return new NodeToolResultWithOutput(result, toolOut, toolErr);
            }
            finally
            {
                System.setOut(originalSysOut);
                System.setErr(originalSysErr);
            }
        });
    }
}
