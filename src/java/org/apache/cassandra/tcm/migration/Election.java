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

package org.apache.cassandra.tcm.migration;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.Startup;
import org.apache.cassandra.tcm.transformations.Register;
import org.apache.cassandra.net.MessageDelivery;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.DistributedMetadataLogKeyspace;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.concurrent.Accumulator;
import org.apache.cassandra.utils.concurrent.CountDownLatch;

public class Election
{
    private static final Logger logger = LoggerFactory.getLogger(Election.class);
    private static final Initiator MIGRATED = new Initiator(null, null);

    private final AtomicReference<Initiator> initiator = new AtomicReference<>();

    public static Election instance = new Election();

    public final PrepareHandler prepareHandler;
    public final AbortHandler abortHandler;

    private final MessageDelivery messaging;

    private Election()
    {
        this(MessagingService.instance());
    }

    private Election(MessageDelivery messaging)
    {
        this.messaging = messaging;
        this.prepareHandler = new PrepareHandler();
        this.abortHandler = new AbortHandler();
    }

    public void nominateSelf(Set<InetAddressAndPort> candidates, Set<InetAddressAndPort> ignoredEndpoints, Function<ClusterMetadata, Boolean> isMatch)
    {
        Set<InetAddressAndPort> sendTo = new HashSet<>(candidates);
        sendTo.removeAll(ignoredEndpoints);
        sendTo.remove(FBUtilities.getBroadcastAddressAndPort());

        try
        {
            initiate(sendTo, isMatch);
            finish(sendTo);
        }
        catch (Exception e)
        {
            abort(sendTo);
            throw e;
        }
    }

    private void initiate(Set<InetAddressAndPort> sendTo, Function<ClusterMetadata, Boolean> isMatch)
    {
        if (!updateInitiator(null, new Initiator(FBUtilities.getBroadcastAddressAndPort(), UUID.randomUUID())))
            throw new IllegalStateException("Migration already initiated by " + initiator.get());

        Collection<Pair<InetAddressAndPort, ClusterMetadataHolder>> metadatas = fanoutAndWait(messaging, sendTo, Verb.TCM_INIT_MIG_REQ, initiator.get());
        if (metadatas.size() != sendTo.size())
        {
            Set<InetAddressAndPort> responded = metadatas.stream().map(p -> p.left).collect(Collectors.toSet());
            String msg = String.format("Did not get response from %s - not continuing with migration. Ignore down hosts with --ignore <host>", Sets.difference(sendTo, responded));
            logger.warn(msg);
            throw new IllegalStateException(msg);
        }

        Set<InetAddressAndPort> mismatching = metadatas.stream().filter(p -> !isMatch.apply(p.right.metadata)).map(p -> p.left).collect(Collectors.toSet());
        if (!mismatching.isEmpty())
        {
            // todo; log the differences between the metadatas
            String msg = String.format("Got mismatching cluster metadatas from %s aborting migration", mismatching);
            throw new IllegalStateException(msg);
        }
    }

    private void finish(Set<InetAddressAndPort> sendTo)
    {
        Initiator currentCoordinator = initiator.get();
        assert currentCoordinator.initiator.equals(FBUtilities.getBroadcastAddressAndPort());

        Startup.initializeAsFirstCMSNode();
        Register.maybeRegister();

        updateInitiator(currentCoordinator, MIGRATED);
        fanoutAndWait(messaging, sendTo, Verb.TCM_NOTIFY_REQ, DistributedMetadataLogKeyspace.getLogState(Epoch.EMPTY));
    }

    private void abort(Set<InetAddressAndPort> sendTo)
    {
        Initiator init = initiator.getAndSet(null);
        for (InetAddressAndPort ep : sendTo)
            messaging.send(Message.out(Verb.TCM_ABORT_MIG, init), ep);
    }

    public Initiator initiator()
    {
        return initiator.get();
    }

    public void migrated()
    {
        initiator.set(MIGRATED);
    }

    private boolean updateInitiator(Initiator expected, Initiator newCoordinator)
    {
        Initiator current = initiator.get();
        return Objects.equals(current, expected) && initiator.compareAndSet(current, newCoordinator);
    }

    public boolean isMigrating()
    {
        Initiator coordinator = initiator();
        return coordinator != null && coordinator != MIGRATED;
    }

    public static <REQ, RSP> Collection<Pair<InetAddressAndPort, RSP>> fanoutAndWait(MessageDelivery messaging, Set<InetAddressAndPort> sendTo, Verb verb, REQ payload)
    {
        Accumulator<Pair<InetAddressAndPort, RSP>> responses = new Accumulator<>(sendTo.size());
        CountDownLatch cdl = CountDownLatch.newCountDownLatch(sendTo.size());
        RequestCallback<RSP> callback = new RequestCallbackWithFailure<RSP>()
        {
            @Override
            public void onResponse(Message<RSP> msg)
            {
                logger.debug("Received a {} response from {}: {}", msg.verb(), msg.from(), msg.payload);
                responses.add(Pair.create(msg.from(), msg.payload));
                cdl.decrement();
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
            {
                logger.debug("Received a failure response from {}: {}", from, reason);
                cdl.decrement();
            }
        };

        sendTo.forEach((ep) -> messaging.sendWithCallback(Message.out(verb, payload), ep, callback));
        cdl.awaitUninterruptibly(DatabaseDescriptor.getCmsAwaitTimeout().to(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
        return responses.snapshot();
    }

    public class PrepareHandler implements IVerbHandler<Initiator>
    {
        @Override
        public void doVerb(Message<Initiator> message) throws IOException
        {
            if (!updateInitiator(null, message.payload))
                throw new IllegalStateException(String.format("Got duplicate initiate migration message from %s, migration is already started by %s", message.from(), initiator()));

            // todo; disallow ANY changes to state managed in ClusterMetadata
            messaging.send(message.responseWith(new ClusterMetadataHolder(message.payload, ClusterMetadata.current())), message.from());
        }
    }

    public class AbortHandler implements IVerbHandler<Initiator>
    {
        @Override
        public void doVerb(Message<Initiator> message) throws IOException
        {
            if (!message.from().equals(initiator().initiator) || !updateInitiator(message.payload, null))
                logger.error("Could not clear initiator - initiator is set to {}, abort message received from {}", initiator(), message.payload);
        }
    }

    public static class Initiator
    {
        public static final Serializer serializer = new Serializer();

        public final InetAddressAndPort initiator;
        public final UUID initToken;

        public Initiator(InetAddressAndPort initiator, UUID initToken)
        {
            this.initiator = initiator;
            this.initToken = initToken;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof Initiator)) return false;
            Initiator other = (Initiator) o;
            return Objects.equals(initiator, other.initiator) && Objects.equals(initToken, other.initToken);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(initiator, initToken);
        }

        @Override
        public String toString()
        {
            return "Initiator{" +
                   "initiator=" + initiator +
                   ", initToken=" + initToken +
                   '}';
        }

        public static class Serializer implements IVersionedSerializer<Initiator>
        {
            @Override
            public void serialize(Initiator t, DataOutputPlus out, int version) throws IOException
            {
                InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serialize(t.initiator, out, version);
                UUIDSerializer.serializer.serialize(t.initToken, out, version);
            }

            @Override
            public Initiator deserialize(DataInputPlus in, int version) throws IOException
            {
                return new Initiator(InetAddressAndPort.Serializer.inetAddressAndPortSerializer.deserialize(in, version),
                                     UUIDSerializer.serializer.deserialize(in, version));
            }

            @Override
            public long serializedSize(Initiator t, int version)
            {
                return InetAddressAndPort.Serializer.inetAddressAndPortSerializer.serializedSize(t.initiator, version) +
                       UUIDSerializer.serializer.serializedSize(t.initToken, version);
            }
        }
    }
}
