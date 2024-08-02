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
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import static org.apache.commons.lang3.StringUtils.EMPTY;

@Command(name = "clearinternalqueue", description = "Clears the internal queue. Be careful before you execute this command, as it will clear the queue and may cause unexpected behavior to the clients, such as timeout, etc.")
public class ClearInternalQueue extends NodeToolCmd
{
    @Option(title = "realclean",
    name = { "-r", "--realclean" },
    description = "Clears the internal queue. Be careful before you execute this command, as it will clear the queue and may cause unexpected behavior to the clients, such as timeout, etc.")
    private boolean realclean = false;

    @Option(title = "queue_name", name = "-q", description = "The name of the queue to clear. For example, Native-Transport-Requests, MutationStage, ReadStage, etc.")

    private String queueName = EMPTY;

    @Override
    public void execute(NodeProbe probe)
    {
        if(queueName.isEmpty())
        {
            throw new RuntimeException("Queue name cannot be empty. Some of the valid names are: Native-Transport-Requests, MutationStage, ReadStage, etc.");
        }
        if (realclean)
        {
            System.out.println("WARNING!!!!! DO NOT USE THIS API FOR PRODUCTION; THIS IS EMERGENCY TOOLING FOR FASTER MITIGATION PURPOSES ONLY");
            /** WARNING!!!!! DO NOT USE THIS API FOR PRODUCTION; THIS IS EMERGENCY
             TOOLING FOR FASTER MITIGATION PURPOSES ONLY  */
            if (probe.internalQueueCleanupEMERGENCYUSEONLY(queueName))
            {
                System.out.printf("Internal queue %s cleared successfully.\n", queueName);
            }
            else
            {
                System.err.printf("Internal queue %s could not be cleared.%n", queueName);
            }
        }
    }
}
