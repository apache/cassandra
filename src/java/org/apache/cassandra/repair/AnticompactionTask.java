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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.AnticompactionRequest;
import org.apache.cassandra.repair.messages.CleanupMessage;
import org.apache.cassandra.utils.SemanticVersion;

public class AnticompactionTask extends AbstractFuture<InetAddress> implements Runnable
{
    /*
     * Version that anticompaction response is not supported up to.
     * If Cassandra version is more than this, we need to wait for anticompaction response.
     */
    private static final SemanticVersion VERSION_CHECKER = new SemanticVersion("2.1.5");

    private final UUID parentSession;
    private final InetAddress neighbor;
    private final boolean doAnticompaction;

    public AnticompactionTask(UUID parentSession, InetAddress neighbor, boolean doAnticompaction)
    {
        this.parentSession = parentSession;
        this.neighbor = neighbor;
        this.doAnticompaction = doAnticompaction;
    }

    public void run()
    {
        AnticompactionRequest acr = new AnticompactionRequest(parentSession);
        SemanticVersion peerVersion = SystemKeyspace.getReleaseVersion(neighbor);
        if (peerVersion != null && peerVersion.compareTo(VERSION_CHECKER) > 0)
        {
            if (doAnticompaction)
            {
                MessagingService.instance().sendRR(acr.createMessage(), neighbor, new AnticompactionCallback(this), TimeUnit.DAYS.toMillis(1), true);
            }
            else
            {
                // we need to clean up parent session
                MessagingService.instance().sendRR(new CleanupMessage(parentSession).createMessage(), neighbor, new AnticompactionCallback(this), TimeUnit.DAYS.toMillis(1), true);
            }
        }
        else
        {
            MessagingService.instance().sendOneWay(acr.createMessage(), neighbor);
            // immediately return after sending request
            set(neighbor);
        }
    }

    /**
     * Callback for antitcompaction request. Run on INTERNAL_RESPONSE stage.
     */
    public static class AnticompactionCallback implements IAsyncCallbackWithFailure
    {
        final AnticompactionTask task;

        public AnticompactionCallback(AnticompactionTask task)
        {
            this.task = task;
        }

        public void response(MessageIn msg)
        {
            task.set(msg.from);
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        public void onFailure(InetAddress from)
        {
            task.setException(new RuntimeException("Anticompaction failed or timed out in " + from));
        }
    }
}
