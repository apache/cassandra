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
package org.apache.cassandra.repair;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.RunnableFuture;

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.db.SnapshotCommand;
import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;

/**
 * SnapshotTask is a task that sends snapshot request.
 */
public class SnapshotTask extends AbstractFuture<InetAddress> implements RunnableFuture<InetAddress>
{
    private final RepairJobDesc desc;
    private final InetAddress endpoint;

    public SnapshotTask(RepairJobDesc desc, InetAddress endpoint)
    {
        this.desc = desc;
        this.endpoint = endpoint;
    }

    public void run()
    {
        MessagingService.instance().sendRR(new SnapshotCommand(desc.keyspace,
                                                               desc.columnFamily,
                                                               desc.sessionId.toString(),
                                                               false).createMessage(),
                                           endpoint,
                                           new SnapshotCallback(this));
    }

    /**
     * Callback for snapshot request. Run on INTERNAL_RESPONSE stage.
     */
    static class SnapshotCallback implements IAsyncCallback
    {
        final SnapshotTask task;

        SnapshotCallback(SnapshotTask task)
        {
            this.task = task;
        }

        /**
         * When we received response from the node,
         *
         * @param msg response received.
         */
        public void response(MessageIn msg)
        {
            task.set(task.endpoint);
        }

        public boolean isLatencyForSnitch() { return false; }
    }
}
