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

import java.util.concurrent.RunnableFuture;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.SnapshotMessage;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.net.Verb.SNAPSHOT_MSG;

/**
 * SnapshotTask is a task that sends snapshot request.
 */
public class SnapshotTask extends AsyncFuture<InetAddressAndPort> implements RunnableFuture<InetAddressAndPort>
{
    private final RepairJobDesc desc;
    private final InetAddressAndPort endpoint;

    SnapshotTask(RepairJobDesc desc, InetAddressAndPort endpoint)
    {
        this.desc = desc;
        this.endpoint = endpoint;
    }

    public void run()
    {
        MessagingService.instance().sendWithCallback(Message.out(SNAPSHOT_MSG, new SnapshotMessage(desc)),
                                                     endpoint,
                                                     new SnapshotCallback(this));
    }

    /**
     * Callback for snapshot request. Run on INTERNAL_RESPONSE stage.
     */
    static class SnapshotCallback implements RequestCallback<InetAddressAndPort>
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
        @Override
        public void onResponse(Message msg)
        {
            task.trySuccess(task.endpoint);
        }

        @Override
        public boolean invokeOnFailure()
        {
            return true;
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            task.tryFailure(new RuntimeException("Could not create snapshot at " + from + "; " + failureReason));
        }
    }
}
