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

package org.apache.cassandra.service.accord.api;

import accord.api.Agent;
import accord.api.Result;
import accord.local.Command;
import accord.local.Node;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getReadRpcTimeout;

public class AccordAgent implements Agent
{
    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        // TODO: this
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        // TODO: this
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        // TODO: this
    }

    @Override
    public void onHandledException(Throwable throwable)
    {
        // TODO: this
    }

    @Override
    public boolean isExpired(TxnId initiated, long now)
    {
        // TODO: should distinguish between reads and writes
        return now - initiated.hlc() > getReadRpcTimeout(MICROSECONDS);
    }
}
