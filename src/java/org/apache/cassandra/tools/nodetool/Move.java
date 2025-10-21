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

import java.io.IOException;

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import static org.apache.commons.lang3.StringUtils.EMPTY;

@Command(name = "move", description = "Move node on the token ring to a new token")
public class Move extends AbstractCommand
{
    @Parameters(paramLabel = "newToken", description = "The new token.", arity = "0..1", index = "0")
    private String newToken = EMPTY;

    @Option(description = "Resume an ongoing move operation", names = { "--resume" })
    private boolean resume;

    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            if (!newToken.isEmpty())
            {
                probe.move(newToken);
            }
            else
            {
                if (resume)
                    probe.resumeMove();
                else
                    throw new IllegalArgumentException("Need to give either a token for a new move operation, or --resume/--abort for an existing one");
            }
        } catch (IOException e)
        {
            throw new RuntimeException("Error during moving node", e);
        }
    }

    @Command(name = "abortmove", description = "Abort a failed move operation for this or a remote node")
    public static class Abort extends AbstractCommand
    {
        @Option(paramLabel = "nodeId", names = { "--node" })
        private String nodeId;

        @Override
        public void execute(NodeProbe probe)
        {
            probe.abortMove(nodeId);
        }
    }
}
