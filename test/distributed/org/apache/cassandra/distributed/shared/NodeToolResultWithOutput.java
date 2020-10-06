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

package org.apache.cassandra.distributed.shared;

import java.io.ByteArrayOutputStream;

import org.apache.cassandra.distributed.api.NodeToolResult;

// Perfer the NodeToolResult that includes output since version 0.0.5
// CASSANDRA-16057
public class NodeToolResultWithOutput
{
    private final NodeToolResult result;
    private final ByteArrayOutputStream stdout;
    private final ByteArrayOutputStream stderr;

    public NodeToolResultWithOutput(NodeToolResult result, ByteArrayOutputStream stdout, ByteArrayOutputStream stderr) {
        this.result = result;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    public NodeToolResult getResult() {
        return this.result;
    }

    public String getStdout() {
        return this.stdout.toString();
    }

    public String getStderr() {
        return this.stderr.toString();
    }
}
