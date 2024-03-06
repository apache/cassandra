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

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.LogState;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.net.*;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.tcm.ClusterMetadataService.State.*;

public class Commit
{
    private static final Logger logger = LoggerFactory.getLogger(Commit.class);

    public static final IVersionedSerializer<Commit> defaultMessageSerializer = new Serializer(NodeVersion.CURRENT.serializationVersion());

    private static volatile Serializer serializerCache;
    public static IVersionedSerializer<Commit> messageSerializer(Version version)
    {
        Serializer cached = serializerCache;
        if (cached != null && cached.serializationVersion.equals(version))
            return cached;
        cached = new Serializer(version);
        serializerCache = cached;
        return cached;
    }

    private final Entry.Id entryId;
    private final Transformation transform;
    private final Epoch lastKnown;

    public Commit(Entry.Id entryId, Transformation transform, Epoch lastKnown)
    {
        this.entryId = entryId;
        this.transform = transform;
        this.lastKnown = lastKnown;
    }

    public String toString()
    {
        return "Commit{" +
               "transformation=" + transform +
               ", lastKnown=" + lastKnown +
               '}';
    }

    static class Serializer implements IVersionedSerializer<Commit>
    {
        private final Version serializationVersion;

        public Serializer(Version serializationVersion)
        {
            this.serializationVersion = serializationVersion;
        }

        public void serialize(Commit t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(serializationVersion.asInt());

            if (serializationVersion.isAtLeast(Version.V2))
                out.writeUnsignedVInt32(ClusterMetadata.current().metadataIdentifier);

            Entry.Id.serializer.serialize(t.entryId, out, serializationVersion);
            Transformation.transformationSerializer.serialize(t.transform, out, serializationVersion);
            Epoch.serializer.serialize(t.lastKnown, out, serializationVersion);
        }

        public Commit deserialize(DataInputPlus in, int version) throws IOException
        {
            Version deserializationVersion = Version.fromInt(in.readUnsignedVInt32());

            if (deserializationVersion.isAtLeast(Version.V2))
                ClusterMetadata.checkIdentifier(in.readUnsignedVInt32());

            Entry.Id entryId = Entry.Id.serializer.deserialize(in, deserializationVersion);
            Transformation transform = Transformation.transformationSerializer.deserialize(in, deserializationVersion);
            Epoch lastKnown = Epoch.serializer.deserialize(in, deserializationVersion);
            return new Commit(entryId, transform, lastKnown);
        }

        public long serializedSize(Commit t, int version)
        {
            int size = TypeSizes.sizeofUnsignedVInt(serializationVersion.asInt());

            if (serializationVersion.isAtLeast(Version.V2))
                size += TypeSizes.sizeofUnsignedVInt(ClusterMetadata.current().metadataIdentifier);

            return size +
                   Transformation.transformationSerializer.serializedSize(t.transform, serializationVersion) +
                   Entry.Id.serializer.serializedSize(t.entryId, serializationVersion) +
                   Epoch.serializer.serializedSize(t.lastKnown, serializationVersion);
        }
    }

    static volatile Result.Serializer resultSerializerCache;
    public interface Result
    {
        IVersionedSerializer<Result> defaultMessageSerializer = new Serializer(NodeVersion.CURRENT.serializationVersion());

        LogState logState();
        boolean isSuccess();
        boolean isFailure();

        default Success success()
        {
            return (Success) this;
        }

        default Failure failure()
        {
            return (Failure) this;
        }

        static IVersionedSerializer<Result> messageSerializer(Version version)
        {
            Serializer cached = resultSerializerCache;
            if (cached != null && cached.serializationVersion.equals(version))
                return cached;
            cached = new Serializer(version);
            resultSerializerCache = cached;
            return cached;
        }

        final class Success implements Result
        {
            public final Epoch epoch;
            public final LogState logState;

            public Success(Epoch epoch, LogState logState)
            {
                this.epoch = epoch;
                this.logState = logState;
            }

            @Override
            public String toString()
            {
                return "Success{" +
                       "epoch=" + epoch +
                       ", logState=" + logState +
                       '}';
            }

            @Override
            public LogState logState()
            {
                return logState;
            }

            public boolean isSuccess()
            {
                return true;
            }

            public boolean isFailure()
            {
                return false;
            }
        }

        static Failure rejected(ExceptionCode exceptionCode, String reason, LogState logState)
        {
            return new Failure(exceptionCode, reason, logState, true);
        }

        static Failure failed(ExceptionCode exceptionCode, String message)
        {
            return new Failure(exceptionCode, message, LogState.EMPTY, false);
        }

        final class Failure implements Result
        {
            public final ExceptionCode code;
            public final String message;
            // Rejection means that we were able to linearize the operation,
            // but it was rejected by the internal logic of the transformation.
            public final boolean rejected;
            public final LogState logState;

            private Failure(ExceptionCode code, String message, LogState logState, boolean rejected)
            {
                if (message == null)
                    message = "";
                this.code = code;
                // TypeSizes#sizeOf encoder only allows strings that are up to Short.MAX_VALUE bytes large
                this.message =  message.substring(0, Math.min(message.length(), Short.MAX_VALUE));
                this.rejected = rejected;
                this.logState = logState;
            }

            @Override
            public String toString()
            {
                return "Failure{" +
                       "code='" + code + '\'' +
                       "message='" + message + '\'' +
                       "rejected=" + rejected +
                       '}';
            }

            @Override
            public LogState logState()
            {
                return logState;
            }

            public boolean isSuccess()
            {
                return false;
            }

            public boolean isFailure()
            {
                return true;
            }
        }

        class Serializer implements IVersionedSerializer<Result>
        {
            private static final byte SUCCESS = 1;
            private static final byte REJECTED = 2;
            private static final byte FAILED = 3;

            private final Version serializationVersion;

            public Serializer(Version serializationVersion)
            {
                this.serializationVersion = serializationVersion;
            }

            @Override
            public void serialize(Result t, DataOutputPlus out, int version) throws IOException
            {
                if (t instanceof Success)
                {
                    out.writeByte(SUCCESS);
                    out.writeUnsignedVInt32(serializationVersion.asInt());
                    LogState.metadataSerializer.serialize(t.logState(), out, serializationVersion);
                    Epoch.serializer.serialize(t.success().epoch, out, serializationVersion);
                }
                else
                {
                    assert t instanceof Failure;
                    Failure failure = (Failure) t;
                    out.writeByte(failure.rejected ? REJECTED : FAILED);
                    out.writeUnsignedVInt32(failure.code.value);
                    out.writeUTF(failure.message);
                    out.writeUnsignedVInt32(serializationVersion.asInt());
                    LogState.metadataSerializer.serialize(t.logState(), out, serializationVersion);
                }
            }

            @Override
            public Result deserialize(DataInputPlus in, int version) throws IOException
            {
                int b = in.readByte();
                if (b == SUCCESS)
                {
                    Version deserializationVersion = Version.fromInt(in.readUnsignedVInt32());
                    LogState delta = LogState.metadataSerializer.deserialize(in, deserializationVersion);
                    Epoch epoch = Epoch.serializer.deserialize(in, deserializationVersion);
                    return new Success(epoch, delta);
                }
                else
                {
                    ExceptionCode exceptionCode = ExceptionCode.fromValue(in.readUnsignedVInt32());
                    String message = in.readUTF();
                    Version deserializationVersion = Version.fromInt(in.readUnsignedVInt32());
                    LogState delta = LogState.metadataSerializer.deserialize(in, deserializationVersion);
                    return new Failure(exceptionCode,
                                       message,
                                       delta,
                                       b == REJECTED);
                }
            }

            @Override
            public long serializedSize(Result t, int version)
            {
                long size = TypeSizes.BYTE_SIZE;
                if (t instanceof Success)
                {
                    size += VIntCoding.computeUnsignedVIntSize(serializationVersion.asInt());
                    size += LogState.metadataSerializer.serializedSize(t.logState(), serializationVersion);
                    size += Epoch.serializer.serializedSize(t.success().epoch, serializationVersion);
                }
                else
                {
                    assert t instanceof Failure;
                    size += VIntCoding.computeUnsignedVIntSize(((Failure) t).code.value);
                    size += TypeSizes.sizeof(((Failure)t).message);
                    size += VIntCoding.computeUnsignedVIntSize(serializationVersion.asInt());
                    size += LogState.metadataSerializer.serializedSize(t.logState(), serializationVersion);
                }
                return size;
            }
        }
    }

    @VisibleForTesting
    public static IVerbHandler<Commit> handlerForTests(Processor processor, Replicator replicator, BiConsumer<Message<?>, InetAddressAndPort> messagingService)
    {
        return new Handler(processor, replicator, messagingService, () -> LOCAL);
    }

    static class Handler implements IVerbHandler<Commit>
    {
        private final Processor processor;
        private final Replicator replicator;
        private final BiConsumer<Message<?>, InetAddressAndPort> messagingService;
        private final Supplier<ClusterMetadataService.State> cmsStateSupplier;

        Handler(Processor processor, Replicator replicator, Supplier<ClusterMetadataService.State> cmsStateSupplier)
        {
            this(processor, replicator, MessagingService.instance()::send, cmsStateSupplier);
        }

        Handler(Processor processor, Replicator replicator, BiConsumer<Message<?>, InetAddressAndPort> messagingService, Supplier<ClusterMetadataService.State> cmsStateSupplier)
        {
            this.processor = processor;
            this.replicator = replicator;
            this.messagingService = messagingService;
            this.cmsStateSupplier = cmsStateSupplier;
        }

        public void doVerb(Message<Commit> message) throws IOException
        {
            checkCMSState();
            logger.info("Received commit request {} from {}", message.payload, message.from());
            Retry.Deadline retryPolicy = Retry.Deadline.at(message.expiresAtNanos(), new Retry.Jitter(TCMMetrics.instance.commitRetries));
            Result result = processor.commit(message.payload.entryId, message.payload.transform, message.payload.lastKnown, retryPolicy);
            if (result.isSuccess())
            {
                Result.Success success = result.success();
                replicator.send(success, message.from());
                logger.info("Responding with full result {} to sender {}", result, message.from());
                // TODO: this response message can get lost; how do we re-discover this on the other side?
                // TODO: what if we have holes after replaying?
                messagingService.accept(message.responseWith(result), message.from());
            }
            else
            {
                Result.Failure failure = result.failure();
                messagingService.accept(message.responseWith(failure), message.from());
            }
        }

        private void checkCMSState()
        {
            switch (cmsStateSupplier.get())
            {
                case RESET:
                case LOCAL:
                    break;
                case REMOTE:
                    throw new NotCMSException("Not currently a member of the CMS, can't commit");
                case GOSSIP:
                    String msg = "Tried to commit when in gossip mode";
                    logger.error(msg);
                    throw new IllegalStateException(msg);
                default:
                    throw new IllegalStateException("Illegal state: " + cmsStateSupplier.get());
            }
        }
    }

    public interface Replicator
    {
        Replicator NO_OP = (a,b) -> {};
        void send(Result result, InetAddressAndPort source);
    }

    public static class DefaultReplicator implements Replicator
    {
        private final Supplier<Directory> directorySupplier;

        public DefaultReplicator(Supplier<Directory> directorySupplier)
        {
            this.directorySupplier = directorySupplier;
        }

        public void send(Result result, InetAddressAndPort source)
        {
            if (!result.isSuccess())
                return;

            Result.Success success = result.success();
            Directory directory = directorySupplier.get();

            // Filter the log entries from the commit result for the purposes of replicating to members of the cluster
            // other than the original submitter. We only need to include the sublist of entries starting at the one
            // which was newly committed. We exclude entries before that one as the submitter may have been lagging and
            // supplied a last known epoch arbitrarily in the past. We include entries after the first newly committed
            // one as there may have been a new period automatically triggered and we'd like to push that out to all
            // peers too. Of course, there may be other entries interspersed with these but it doesn't harm anything to
            // include those too, it may simply be redundant.
            LogState newlyCommitted = success.logState.retainFrom(success.epoch);
            assert !newlyCommitted.isEmpty() : String.format("Nothing to replicate after retaining epochs since %s from %s",
                                                             success.epoch, success.logState);

            for (NodeId peerId : directory.peerIds())
            {
                InetAddressAndPort endpoint = directory.endpoint(peerId);
                boolean upgraded = directory.version(peerId).isUpgraded();
                // Do not replicate to self and to the peer that has requested to commit this message
                if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()) ||
                    (source != null && source.equals(endpoint)) ||
                    !upgraded)
                {
                    continue;
                }

                logger.info("Replicating newly committed transformations up to {} to {}", newlyCommitted, endpoint);
                MessagingService.instance().send(Message.out(Verb.TCM_REPLICATION, newlyCommitted), endpoint);
            }
        }
    }

}
