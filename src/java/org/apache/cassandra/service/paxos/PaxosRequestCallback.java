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
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.exceptions.RetryOnDifferentSystemException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.FailureRecordingCallback;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.util.concurrent.Futures.getUnchecked;
import static org.apache.cassandra.exceptions.RequestFailure.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM;
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
        if (DatabaseDescriptor.getAccordTransactionsEnabled())
            ClusterMetadataService.instance().fetchLogFromPeerOrCMS(message.from(), message.epoch());
        onResponse(message.payload, message.from());
    }

    protected <I> void executeOnSelf(I parameter, Function<I, T> execute)
    {
        T response;
        try
        {
            response = execute.apply(parameter);
            if (response == null)
                return;
        }
        catch (RetryOnDifferentSystemException e)
        {
            onFailure(getBroadcastAddressAndPort(), RequestFailure.RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM);
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

    protected <I, J> void executeOnSelfAsync(I parameter1, J parameter2, BiFunction<I, J, Future<T>> execute)
    {
        try
        {
            Future<T> responseFuture = execute.apply(parameter1, parameter2);
            if (responseFuture == null)
                return;

            if (responseFuture.isDone())
            {
                // Fast path: future already complete
                T response = getUnchecked(responseFuture);
                onResponse(response, getBroadcastAddressAndPort());
            }
            else
            {
                // Async path: add callback for when future completes
                responseFuture.addCallback((response, failure) -> {
                    if (failure != null)
                    {
                        RequestFailure reason = UNKNOWN;
                        if (failure instanceof WriteTimeoutException)
                            reason = TIMEOUT;
                        else if (failure instanceof RetryOnDifferentSystemException)
                            reason = RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM;
                        else
                            logger.error("Failed to apply {} locally", parameter1, failure);

                        onFailure(getBroadcastAddressAndPort(), reason);
                    }
                    else
                    {
                        onResponse(response, getBroadcastAddressAndPort());
                    }
                });
            }
        }
        catch (Exception ex)
        {
            RequestFailure reason = UNKNOWN;
            if (ex instanceof WriteTimeoutException)
                reason = TIMEOUT;
            else if (ex instanceof RetryOnDifferentSystemException)
                reason = RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM;
            else
                logger.error("Failed to apply {} locally", parameter1, ex);

            onFailure(getBroadcastAddressAndPort(), reason);
        }
    }

    static boolean shouldExecuteOnSelf(InetAddressAndPort replica)
    {
        return USE_SELF_EXECUTION && replica.equals(getBroadcastAddressAndPort());
    }
}
