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

import javax.inject.Inject;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;

/**
 * Base class for all nodetool commands.
 */
public abstract class AbstractCommand implements Runnable
{
    @Inject
    protected Output logger;

    protected NodeProbe probe;

    public void probe(NodeProbe probe)
    {
        this.probe = probe;
    }

    public NodeProbe probe()
    {
        return probe;
    }

    @Override
    public void run()
    {
        execute(probe);
    }

    protected abstract void execute(NodeProbe probe);
}
