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

package org.apache.cassandra.service.paxos;

import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.FailureRecordingCallback;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.apache.cassandra.exceptions.RequestFailure.TIMEOUT;
import static org.apache.cassandra.exceptions.RequestFailure.UNKNOWN;
import static org.apache.cassandra.utils.FBUtilities.getBroadcastAddressAndPort;

public abstract class PaxosRequestCallback<T> extends FailureRecordingCallback<T>
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosRequestCallback.class);
    private static final boolean USE_SELF_EXECUTION = CassandraRelevantProperties.PAXOS_USE_SELF_EXECUTION.getBoolean();

    protected abstract void onResponse(T response, InetAddressAndPort from);

    @Override
    public void onResponse(Message<T> message)
    {
        ClusterMetadataService.instance().fetchLogFromCMS(message.epoch());
        onResponse(message.payload, message.from());
    }

    protected <I> void executeOnSelf(I parameter, BiFunction<I, InetAddressAndPort, T> execute)
    {
        T response;
        try
        {
            response = execute.apply(parameter, getBroadcastAddressAndPort());
            if (response == null)
                return;
        }
        catch (Exception ex)
        {
            RequestFailure reason = UNKNOWN;
            if (ex instanceof WriteTimeoutException) reason = TIMEOUT;
            else logger.error("Failed to apply {} locally", parameter, ex);

            onFailure(getBroadcastAddressAndPort(), reason);
            return;
        }

        onResponse(response, getBroadcastAddressAndPort());
    }

    public interface TriFunction<A, B, C, D> {
        D apply(A var1, B var2, C var3);
    }

    protected <I, J> void executeOnSelf(I parameter1, J parameter2, TriFunction<I, J, InetAddressAndPort, T> execute)
    {
        T response;
        try
        {
            response = execute.apply(parameter1, parameter2, getBroadcastAddressAndPort());
            if (response == null)
                return;
        }
        catch (Exception ex)
        {
            RequestFailure reason = UNKNOWN;
            if (ex instanceof WriteTimeoutException) reason = TIMEOUT;
            else logger.error("Failed to apply {}, {} locally", parameter1, parameter2, ex);

            onFailure(getBroadcastAddressAndPort(), reason);
            return;
        }

        onResponse(response, getBroadcastAddressAndPort());
    }

    static boolean shouldExecuteOnSelf(InetAddressAndPort replica)
    {
        return USE_SELF_EXECUTION && replica.equals(getBroadcastAddressAndPort());
    }
}
