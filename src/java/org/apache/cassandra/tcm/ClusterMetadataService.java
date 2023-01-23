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

package org.apache.cassandra.tcm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.service.ConsensusMigrationStateStore;
import org.apache.cassandra.service.ConsensusMigrationStateStore.MigrationStateSnapshot;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static org.apache.cassandra.net.NoPayload.noPayload;
import static org.apache.cassandra.net.Verb.REQUEST_CM;
import static org.apache.cassandra.net.Verb.UPDATE_CM;

public class ClusterMetadataService
{
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataService.class);

    public static final ClusterMetadataService instance = new ClusterMetadataService();

    public volatile ClusterMetadata metadata = new ClusterMetadata(Epoch.FIRST, new ConsensusMigrationStateStore.MigrationStateSnapshot(ImmutableMap.of(), Epoch.FIRST));

    @VisibleForTesting
    public static void reset()
    {
        instance.metadata = new ClusterMetadata(instance.metadata.epoch.nextEpoch(false), new ConsensusMigrationStateStore.MigrationStateSnapshot(ImmutableMap.of(), Epoch.FIRST));
        AccordService.instance().createEpochFromConfigUnsafe();
    }

    public ClusterMetadata metadata()
    {
        return metadata;
    }

    public synchronized ClusterMetadata commit(Transformation transform)
    {
        Transformation.Result result = transform.execute(metadata);
        if (result.isSuccess())
        {
            metadata = result.success().metadata;
            AccordService.instance().createEpochFromConfigUnsafe();
            propagateUpdate();
        }
        return metadata;
    }

    private void propagateUpdate()
    {
        Message<MigrationStateSnapshot> out = Message.out(UPDATE_CM, metadata.migrationStateSnapshot);
        List<Future<?>> results = new ArrayList<>();
        for (InetAddressAndPort endpoint : StorageService.instance.getTokenMetadata().getAllEndpoints())
        {
            if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
                continue;
            results.add(MessagingService.instance().sendWithResult(out, endpoint));
        }
        try
        {
            FutureCombiner.allOf(results).get();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public ClusterMetadata maybeCatchup(Epoch theirEpoch)
    {
        if (theirEpoch.compareTo(metadata.epoch) > 0)
        {
            Message<NoPayload> out = Message.out(REQUEST_CM, noPayload);
            List<Future<MigrationStateSnapshot>> results = new ArrayList<>();
            for (InetAddressAndPort endpoint : StorageService.instance.getTokenMetadata().getAllEndpoints())
            {
                if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
                    continue;
                results.add(MessagingService.instance().<MigrationStateSnapshot>sendWithResult(out, endpoint).map(m -> m.payload));
            }
            try
            {
                for (MigrationStateSnapshot snapshot : FutureCombiner.allOf(results).get())
                {
                    if (snapshot.epoch.compareTo(metadata.epoch) > 0)
                        metadata = new ClusterMetadata(snapshot.epoch, snapshot);
                }
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }

        if (metadata.epoch.compareTo(theirEpoch) < 0)
            throw new IllegalStateException("Unable to find updated cluster metadata that should exist");

        return metadata;
    }

    public static final IVerbHandler<MigrationStateSnapshot> updateHandler = message ->
    {
        try
        {
            instance.metadata = new ClusterMetadata(message.payload.epoch, message.payload);
            AccordService.instance().createEpochFromConfigUnsafe();
            MessagingService.instance().respond(noPayload, message);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            MessagingService.instance().respondWithFailure(RequestFailureReason.forException(t), message);
        }
    };

    public static final IVerbHandler<NoPayload> catchupHandler = message ->
    {
        try
        {
            MessagingService.instance().respond(ClusterMetadata.current().migrationStateSnapshot, message);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            MessagingService.instance().respondWithFailure(RequestFailureReason.forException(t), message);
        }
    };
}
