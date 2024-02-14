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

package org.apache.cassandra.tools.nodetool.clustermetadata;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.tcm.CMSOperations;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tools.ToolRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class DumpClusterMetadataTest extends CQLTester
{
    @BeforeClass
    public static void setup() throws Exception
    {
        requireNetwork();
        startJMXServer();
        CMSOperations.initJmx();
    }

    @Test
    public void testDumpClusterMetadata()
    {

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("cms", "dump");
        tool.assertOnCleanExit();

        assertThat(tool.getStdout()).contains("Cluster Metadata dumped to file:");
    }

    @Test
    public void testDumpClusterMetdataWithArguments() throws IOException
    {
        long epoch = Epoch.EMPTY.getEpoch();
        Epoch transformEpoch = ClusterMetadata.current().epoch;
        String version = NodeVersion.CURRENT.serializationVersion().toString();

        ToolRunner.ToolResult tool = ToolRunner.invokeNodetool("cms", "dump",
                                                               "-e", String.valueOf(epoch),
                                                               "-te", String.valueOf(transformEpoch.getEpoch()),
                                                               "-v", version);
        tool.assertOnCleanExit();

        String output = tool.getStdout();
        String marker = "Cluster Metadata dumped to file:";
        assertThat(output).contains(marker);

        int beginIndex = output.indexOf(marker) + marker.length();
        String dumpFilePath = output.substring(beginIndex, output.indexOf('\n', beginIndex)).trim();
        ClusterMetadata metadata = loadClusterMetadata(dumpFilePath);
        assertThat(metadata.epoch).isEqualTo(transformEpoch);
    }


    @Test
    public void testDumpClusterMetdataWithInvalidArguments() throws IOException
    {
        long epoch = Epoch.EMPTY.getEpoch();
        Epoch transformEpoch = ClusterMetadata.current().epoch;
        String version = NodeVersion.CURRENT.serializationVersion().toString();

        // transform-epoch argument isn't passed
        ToolRunner.ToolResult result = ToolRunner.invokeNodetool("cms", "dump",
                                                                 "-e", String.valueOf(epoch),
                                                                 "-v", version);
        assertThat(result.getExitCode()).isEqualTo(1);
        result.asserts().errorContains("transform-epoch");


        // epoch isn't passed
        result = ToolRunner.invokeNodetool("cms", "dump",
                                           "-te", String.valueOf(transformEpoch.getEpoch()),
                                           "-v", version);
        assertThat(result.getExitCode()).isEqualTo(1);
        result.asserts().errorContains("epoch");

        // version isn't passed
        result = ToolRunner.invokeNodetool("cms", "dump",
                                           "-e", String.valueOf(epoch),
                                           "-te", String.valueOf(transformEpoch.getEpoch()));
        assertThat(result.getExitCode()).isEqualTo(1);
        result.asserts().errorContains("version");

        // Two arguments aren't passed
        result = ToolRunner.invokeNodetool("cms", "dump",
                                           "-e", String.valueOf(epoch));
        result.asserts().errorContains("version");
        result.asserts().errorContains("transform-epoch");
    }


    private ClusterMetadata loadClusterMetadata(String clusterMetdataFilePath) throws IOException
    {
        return ClusterMetadataService.deserializeClusterMetadata(clusterMetdataFilePath);
    }
}
