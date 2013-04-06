/**
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

package org.apache.cassandra.streaming;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.*;

public abstract class AbstractStreamSession implements IEndpointStateChangeSubscriber, IFailureDetectionEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractStreamSession.class);

    protected final InetAddress host;
    protected final UUID sessionId;
    protected String table;
    protected final IStreamCallback callback;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    protected AbstractStreamSession(String table, InetAddress host, UUID sessionId, IStreamCallback callback)
    {
        this.host = host;
        this.sessionId = sessionId;
        this.table = table;
        this.callback = callback;
        Gossiper.instance.register(this);
        FailureDetector.instance.registerFailureDetectionEventListener(this);
    }

    public UUID getSessionId()
    {
        return sessionId;
    }

    public InetAddress getHost()
    {
        return host;
    }

    public void close(boolean success)
    {
        if (!isClosed.compareAndSet(false, true))
        {
            logger.debug("Stream session {} already closed", getSessionId());
            return;
        }

        closeInternal(success);

        Gossiper.instance.unregister(this);
        FailureDetector.instance.unregisterFailureDetectionEventListener(this);

        logger.debug("closing with status " + success);
        if (callback != null)
        {
            if (success)
                callback.onSuccess();
            else
                callback.onFailure();
        }
    }

    protected abstract void closeInternal(boolean success);

    public void onJoin(InetAddress endpoint, EndpointState epState) {}
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
        if (!endpoint.equals(getHost()))
            return;

        // We want a higher confidence in the failure detection than usual because failing a streaming wrongly has a high cost.
        if (phi < 2 * DatabaseDescriptor.getPhiConvictThreshold())
            return;

        logger.error("Stream failed because {} died or was restarted/removed (streams may still be active "
                      + "in background, but further streams won't be started)", endpoint);
        close(false);
    }
}
