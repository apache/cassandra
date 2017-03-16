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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.AbstractFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.AnticompactionRequest;
import org.apache.cassandra.utils.CassandraVersion;

public class AnticompactionTask extends AbstractFuture<InetAddress> implements Runnable, IEndpointStateChangeSubscriber,
                                                                               IFailureDetectionEventListener
{
    /*
     * Version that anticompaction response is not supported up to.
     * If Cassandra version is more than this, we need to wait for anticompaction response.
     */
    private static final CassandraVersion VERSION_CHECKER = new CassandraVersion("2.1.5");
    private static Logger logger = LoggerFactory.getLogger(AnticompactionTask.class);

    private final UUID parentSession;
    private final InetAddress neighbor;
    private final Collection<Range<Token>> successfulRanges;
    private final AtomicBoolean isFinished = new AtomicBoolean(false);

    public AnticompactionTask(UUID parentSession, InetAddress neighbor, Collection<Range<Token>> successfulRanges)
    {
        this.parentSession = parentSession;
        this.neighbor = neighbor;
        this.successfulRanges = successfulRanges;
    }

    public void run()
    {
        if (FailureDetector.instance.isAlive(neighbor))
        {
            AnticompactionRequest acr = new AnticompactionRequest(parentSession, successfulRanges);
            CassandraVersion peerVersion = SystemKeyspace.getReleaseVersion(neighbor);
            if (peerVersion != null && peerVersion.compareTo(VERSION_CHECKER) > 0)
            {
                MessagingService.instance().sendRR(acr.createMessage(), neighbor, new AnticompactionCallback(this), TimeUnit.DAYS.toMillis(1), true);
            }
            else
            {
                // immediately return after sending request
                MessagingService.instance().sendOneWay(acr.createMessage(), neighbor);
                maybeSetResult(neighbor);
            }
        }
        else
        {
            maybeSetException(new IOException(neighbor + " is down"));
        }
    }

    private boolean maybeSetException(Throwable t)
    {
        if (isFinished.compareAndSet(false, true))
        {
            setException(t);
            return true;
        }
        return false;
    }

    private boolean maybeSetResult(InetAddress o)
    {
        if (isFinished.compareAndSet(false, true))
        {
            set(o);
            return true;
        }
        return false;
    }

    /**
     * Callback for antitcompaction request. Run on INTERNAL_RESPONSE stage.
     */
    public class AnticompactionCallback implements IAsyncCallbackWithFailure
    {
        final AnticompactionTask task;

        public AnticompactionCallback(AnticompactionTask task)
        {
            this.task = task;
        }

        public void response(MessageIn msg)
        {
            maybeSetResult(msg.from);
        }

        public boolean isLatencyForSnitch()
        {
            return false;
        }

        public void onFailure(InetAddress from, RequestFailureReason failureReason)
        {
            maybeSetException(new RuntimeException("Anticompaction failed or timed out in " + from));
        }
    }

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) {}
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {}
    public void onAlive(InetAddress endpoint, EndpointState state) {}
    public void onDead(InetAddress endpoint, EndpointState state) {}

    public void onRemove(InetAddress endpoint)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void onRestart(InetAddress endpoint, EndpointState epState)
    {
        convict(endpoint, Double.MAX_VALUE);
    }

    public void convict(InetAddress endpoint, double phi)
    {
        if (!neighbor.equals(endpoint))
            return;

        // We want a higher confidence in the failure detection than usual because failing a repair wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        Exception exception = new IOException(String.format("Endpoint %s died during anti-compaction.", endpoint));
        if (maybeSetException(exception))
        {
            // Though unlikely, it is possible to arrive here multiple time and we want to avoid print an error message twice
            logger.error("[repair #{}] Endpoint {} died during anti-compaction", endpoint, parentSession, exception);
        }
    }
}
