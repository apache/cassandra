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
import java.io.IOException;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "enablebackup", description = "Enable incremental backup with specified keyspace and table")
public class EnableBackup extends NodeToolCmd
{   
    @Option(title = "ktlist", name = { "-kt", "--kt-list" }, description = "The list of Keyspace.table connected by ',' to take incremental backup. All table will take backup if not set. ")
    private String ktList = null;
    
    @Override
    public void execute(NodeProbe probe)
    {
        try 
        {
            System.out.printf("%s%n", "Requested enable encremental backup for :");
            if (null != ktList && !ktList.isEmpty())
            {
                ktList = ktList.replace(" ", "");
                probe.setIncrementalBackupsEnabled(true, ktList.split(","));
                System.out.printf("    %s", probe.getIncrementalBackupsKSTBs());
            }
            else 
            {
                probe.setIncrementalBackupsEnabled(true);
                System.out.printf("    %s", "All keyspaces");
            }
        }
        catch (IOException | IllegalArgumentException e) 
        {
            throw new RuntimeException("Error during enable incremental backup.", e);
        }
    }
}