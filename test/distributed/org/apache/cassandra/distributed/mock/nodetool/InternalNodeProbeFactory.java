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

package org.apache.cassandra.distributed.mock.nodetool;

import java.io.IOException;

import org.apache.cassandra.tools.INodeProbeFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;

public class InternalNodeProbeFactory implements INodeProbeFactory
{
    private final boolean withNotifications;
    private final Output output;

    public InternalNodeProbeFactory(boolean withNotifications, Output output)
    {
        this.withNotifications = withNotifications;
        this.output = output;
    }

    public NodeProbe create(String host, int port) throws IOException {
        return createInternalNodeProbe(withNotifications, output);
    }

    public NodeProbe create(String host, int port, String username, String password) throws IOException {
        return createInternalNodeProbe(withNotifications, output);
    }

    private static NodeProbe createInternalNodeProbe(boolean withNotifications, Output output) {
        NodeProbe probe = new InternalNodeProbe(withNotifications);
        probe.setOutput(output);
        return probe;
    }
}
