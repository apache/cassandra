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

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VERSION;
import static org.apache.cassandra.config.CassandraRelevantProperties.JAVA_VM_NAME;

@Command(name = "version", description = "Print cassandra version")
public class Version extends AbstractCommand
{
    @Option(paramLabel = "verbose",
            names = { "-v", "--verbose" },
            description = "Include additional information")
    private boolean verbose = false;

    @Override
    public void execute(NodeProbe probe)
    {
        probe.output().out.println("ReleaseVersion: " + probe.getReleaseVersion());
        if (verbose)
        {
            probe.output().out.println("BuildDate: " + probe.getBuildDate());
            probe.output().out.println("GitSHA: " + probe.getGitSHA());
            probe.output().out.printf("JVM vendor/version: %s/%s%n", JAVA_VM_NAME.getString(), JAVA_VERSION.getString());
        }
    }
}
