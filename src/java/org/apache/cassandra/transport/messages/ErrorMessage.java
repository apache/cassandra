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
package org.apache.cassandra.transport.messages;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.CodecException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.transport.*;
import org.apache.cassandra.utils.MD5Digest;

/**
 * Message to indicate an error to the client.
 */
public class ErrorMessage extends Message.Response
{
    private static final Logger logger = LoggerFactory.getLogger(ErrorMessage.class);

    public static final Message.Codec<ErrorMessage> codec = new Message.Codec<ErrorMessage>()
    {
        public ErrorMessage decode(ByteBuf body, ProtocolVersion version)
        {
            ExceptionCode code = ExceptionCode.fromValue(body.readInt());
            String msg = CBUtil.readString(body);

            TransportException te = null;
            switch (code)
            {
                case SERVER_ERROR:
                    te = new ServerError(msg);
                    break;
                case PROTOCOL_ERROR:
                    te = new ProtocolException(msg);
                    break;
                case BAD_CREDENTIALS:
                    te = new AuthenticationException(msg);
                    break;
                case UNAVAILABLE:
                    {
                        ConsistencyLevel cl = CBUtil.readConsistencyLevel(body);
                        int required = body.readInt();
                        int alive = body.readInt();
                        te = UnavailableException.create(cl, required, alive);
                    }
                    break;
                case OVERLOADED:
                    te = new OverloadedException(msg);
                    break;
                case IS_BOOTSTRAPPING:
                    te = new IsBootstrappingException();
                    break;
                case TRUNCATE_ERROR:
                    te = new TruncateException(msg);
                    break;
                case WRITE_FAILURE:
                case READ_FAILURE:
                    {
                        ConsistencyLevel cl = CBUtil.readConsistencyLevel(body);
                        int received = body.readInt();
                        int blockFor = body.readInt();
                        // The number of failures is also present in protocol v5, but used instead to specify the size of the failure map
                        int failure = body.readInt();

                        Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint;
                        if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                        {
                            ImmutableMap.Builder<InetAddressAndPort, RequestFailureReason> builder = ImmutableMap.builderWithExpectedSize(failure);
                            for (int i = 0; i < failure; i++)
                            {
                                InetAddress endpoint = CBUtil.readInetAddr(body);
                                RequestFailureReason failureReason = RequestFailureReason.fromCode(body.readUnsignedShort());
                                builder.put(InetAddressAndPort.getByAddress(endpoint), failureReason);
                            }
                            failureReasonByEndpoint = builder.build();
                        }
                        else
                        {
                            failureReasonByEndpoint = Collections.emptyMap();
                        }

                        if (code == ExceptionCode.WRITE_FAILURE)
                        {
                            WriteType writeType = Enum.valueOf(WriteType.class, CBUtil.readString(body));
                            te = new WriteFailureException(cl, received, blockFor, writeType, failureReasonByEndpoint);
                        }
                        else
                        {
                            byte dataPresent = body.readByte();
                            te = new ReadFailureException(cl, received, blockFor, dataPresent != 0, failureReasonByEndpoint);
                        }
                    }
                    break;
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    {
                        ConsistencyLevel cl = CBUtil.readConsistencyLevel(body);
                        int received = body.readInt();
                        int blockFor = body.readInt();
                        if (code == ExceptionCode.WRITE_TIMEOUT)
                        {
                            WriteType writeType = Enum.valueOf(WriteType.class, CBUtil.readString(body));
                            if (version.isGreaterOrEqualTo(ProtocolVersion.V5) && writeType == WriteType.CAS)
                            {
                                int contentions = body.readShort();
                                te = new CasWriteTimeoutException(writeType, cl, received, blockFor, contentions);
                            }
                            else
                            {
                                te = new WriteTimeoutException(writeType, cl, received, blockFor);
                            }
                        }
                        else
                        {
                            byte dataPresent = body.readByte();
                            te = new ReadTimeoutException(cl, received, blockFor, dataPresent != 0);
                        }
                        break;
                    }
                case FUNCTION_FAILURE:
                    String fKeyspace = CBUtil.readString(body);
                    String fName = CBUtil.readString(body);
                    List<String> argTypes = CBUtil.readStringList(body);
                    te = FunctionExecutionException.create(new FunctionName(fKeyspace, fName), argTypes, msg);
                    break;
                case UNPREPARED:
                    {
                        MD5Digest id = MD5Digest.wrap(CBUtil.readBytes(body));
                        te = new PreparedQueryNotFoundException(id);
                    }
                    break;
                case SYNTAX_ERROR:
                    te = new SyntaxException(msg);
                    break;
                case UNAUTHORIZED:
                    te = new UnauthorizedException(msg);
                    break;
                case INVALID:
                    te = new InvalidRequestException(msg);
                    break;
                case CONFIG_ERROR:
                    te = new ConfigurationException(msg);
                    break;
                case CDC_WRITE_FAILURE:
                    te = new CDCWriteException(msg);
                    break;
                case ALREADY_EXISTS:
                    String ksName = CBUtil.readString(body);
                    String cfName = CBUtil.readString(body);
                    if (cfName.isEmpty())
                        te = new AlreadyExistsException(ksName);
                    else
                        te = new AlreadyExistsException(ksName, cfName);
                    break;
                case CAS_WRITE_UNKNOWN:
                    assert version.isGreaterOrEqualTo(ProtocolVersion.V5);

                    ConsistencyLevel cl = CBUtil.readConsistencyLevel(body);
                    int received = body.readInt();
                    int blockFor = body.readInt();
                    te = new CasWriteUnknownResultException(cl, received, blockFor);
                    break;
            }
            return new ErrorMessage(te);
        }

        public void encode(ErrorMessage msg, ByteBuf dest, ProtocolVersion version)
        {
            final TransportException err = getBackwardsCompatibleException(msg, version);
            dest.writeInt(err.code().value);
            String errorString = err.getMessage() == null ? "" : err.getMessage();
            CBUtil.writeString(errorString, dest);

            switch (err.code())
            {
                case UNAVAILABLE:
                    UnavailableException ue = (UnavailableException)err;
                    CBUtil.writeConsistencyLevel(ue.consistency, dest);
                    dest.writeInt(ue.required);
                    dest.writeInt(ue.alive);
                    break;
                case WRITE_FAILURE:
                case READ_FAILURE:
                    {
                        RequestFailureException rfe = (RequestFailureException) err;

                        CBUtil.writeConsistencyLevel(rfe.consistency, dest);
                        dest.writeInt(rfe.received);
                        dest.writeInt(rfe.blockFor);
                        // The number of failures is also present in protocol v5, but used instead to specify the size of the failure map
                        dest.writeInt(rfe.failureReasonByEndpoint.size());

                        if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                        {
                            for (Map.Entry<InetAddressAndPort, RequestFailureReason> entry : rfe.failureReasonByEndpoint.entrySet())
                            {
                                CBUtil.writeInetAddr(entry.getKey().getAddress(), dest);
                                dest.writeShort(entry.getValue().code);
                            }
                        }

                        if (err.code() == ExceptionCode.WRITE_FAILURE)
                            CBUtil.writeAsciiString(((WriteFailureException) rfe).writeType.toString(), dest);
                        else
                            dest.writeByte((byte) (((ReadFailureException) rfe).dataPresent ? 1 : 0));
                        break;
                    }
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    RequestTimeoutException rte = (RequestTimeoutException)err;

                    CBUtil.writeConsistencyLevel(rte.consistency, dest);
                    dest.writeInt(rte.received);
                    dest.writeInt(rte.blockFor);
                    if (err.code() == ExceptionCode.WRITE_TIMEOUT)
                    {
                        CBUtil.writeAsciiString(((WriteTimeoutException)rte).writeType.toString(), dest);
                        // CasWriteTimeoutException already implies protocol V5, but double check to be safe.
                        if (version.isGreaterOrEqualTo(ProtocolVersion.V5) && rte instanceof CasWriteTimeoutException)
                            dest.writeShort(((CasWriteTimeoutException)rte).contentions);
                    }
                    else
                    {
                        dest.writeByte((byte)(((ReadTimeoutException)rte).dataPresent ? 1 : 0));
                    }
                    break;
                case FUNCTION_FAILURE:
                    FunctionExecutionException fee = (FunctionExecutionException)msg.error;
                    CBUtil.writeAsciiString(fee.functionName.keyspace, dest);
                    CBUtil.writeAsciiString(fee.functionName.name, dest);
                    CBUtil.writeStringList(fee.argTypes, dest);
                    break;
                case UNPREPARED:
                    PreparedQueryNotFoundException pqnfe = (PreparedQueryNotFoundException)err;
                    CBUtil.writeBytes(pqnfe.id.bytes, dest);
                    break;
                case ALREADY_EXISTS:
                    AlreadyExistsException aee = (AlreadyExistsException)err;
                    CBUtil.writeAsciiString(aee.ksName, dest);
                    CBUtil.writeAsciiString(aee.cfName, dest);
                    break;
                case CAS_WRITE_UNKNOWN:
                    assert version.isGreaterOrEqualTo(ProtocolVersion.V5);
                    CasWriteUnknownResultException cwue = (CasWriteUnknownResultException)err;
                    CBUtil.writeConsistencyLevel(cwue.consistency, dest);
                    dest.writeInt(cwue.received);
                    dest.writeInt(cwue.blockFor);
            }
        }

        public int encodedSize(ErrorMessage msg, ProtocolVersion version)
        {
            TransportException err = getBackwardsCompatibleException(msg, version);
            String errorString = err.getMessage() == null ? "" : err.getMessage();
            int size = 4 + CBUtil.sizeOfString(errorString);
            switch (err.code())
            {
                case UNAVAILABLE:
                    UnavailableException ue = (UnavailableException)err;
                    size += CBUtil.sizeOfConsistencyLevel(ue.consistency) + 8;
                    break;
                case WRITE_FAILURE:
                case READ_FAILURE:
                    {
                        RequestFailureException rfe = (RequestFailureException)err;

                        size += CBUtil.sizeOfConsistencyLevel(rfe.consistency) + 4 + 4 + 4;
                        if (err.code() == ExceptionCode.WRITE_FAILURE)
                            size += CBUtil.sizeOfAsciiString(((WriteFailureException)rfe).writeType.toString());
                        else
                            size += 1;

                        if (version.isGreaterOrEqualTo(ProtocolVersion.V5))
                        {
                            for (Map.Entry<InetAddressAndPort, RequestFailureReason> entry : rfe.failureReasonByEndpoint.entrySet())
                            {
                                size += CBUtil.sizeOfInetAddr(entry.getKey().getAddress());
                                size += 2; // RequestFailureReason code
                            }
                        }
                    }
                    break;
                case WRITE_TIMEOUT:
                case READ_TIMEOUT:
                    RequestTimeoutException rte = (RequestTimeoutException)err;
                    boolean isWrite = err.code() == ExceptionCode.WRITE_TIMEOUT;
                    size += CBUtil.sizeOfConsistencyLevel(rte.consistency) + 8;
                    if (isWrite)
                        size += CBUtil.sizeOfAsciiString(((WriteTimeoutException)rte).writeType.toString());
                    else
                        size += 1;

                    // CasWriteTimeoutException already implies protocol V5, but double check to be safe.
                    if (isWrite && version.isGreaterOrEqualTo(ProtocolVersion.V5) && rte instanceof CasWriteTimeoutException)
                        size += 2; // CasWriteTimeoutException appends a short for contentions occured.
                    break;
                case FUNCTION_FAILURE:
                    FunctionExecutionException fee = (FunctionExecutionException)msg.error;
                    size += CBUtil.sizeOfAsciiString(fee.functionName.keyspace);
                    size += CBUtil.sizeOfAsciiString(fee.functionName.name);
                    size += CBUtil.sizeOfStringList(fee.argTypes);
                    break;
                case UNPREPARED:
                    PreparedQueryNotFoundException pqnfe = (PreparedQueryNotFoundException)err;
                    size += CBUtil.sizeOfBytes(pqnfe.id.bytes);
                    break;
                case ALREADY_EXISTS:
                    AlreadyExistsException aee = (AlreadyExistsException)err;
                    size += CBUtil.sizeOfAsciiString(aee.ksName);
                    size += CBUtil.sizeOfAsciiString(aee.cfName);
                    break;
                case CAS_WRITE_UNKNOWN:
                    assert version.isGreaterOrEqualTo(ProtocolVersion.V5);
                    CasWriteUnknownResultException cwue = (CasWriteUnknownResultException)err;
                    size += CBUtil.sizeOfConsistencyLevel(cwue.consistency) + 4 + 4; // receivedFor: 4, blockFor: 4
                    break;
            }
            return size;
        }
    };

    private static TransportException getBackwardsCompatibleException(ErrorMessage msg, ProtocolVersion version)
    {
        if (version.isSmallerThan(ProtocolVersion.V4))
        {
            switch (msg.error.code())
            {
                case READ_FAILURE:
                    ReadFailureException rfe = (ReadFailureException) msg.error;
                    return new ReadTimeoutException(rfe.consistency, rfe.received, rfe.blockFor, rfe.dataPresent);
                case WRITE_FAILURE:
                    WriteFailureException wfe = (WriteFailureException) msg.error;
                    return new WriteTimeoutException(wfe.writeType, wfe.consistency, wfe.received, wfe.blockFor);
                case FUNCTION_FAILURE:
                case CDC_WRITE_FAILURE:
                    return new InvalidRequestException(msg.toString());
            }
        }

        if (version.isSmallerThan(ProtocolVersion.V5))
        {
            switch (msg.error.code())
            {
                case WRITE_TIMEOUT:
                    if (msg.error instanceof CasWriteTimeoutException)
                    {
                        CasWriteTimeoutException cwte = (CasWriteTimeoutException) msg.error;
                        return new WriteTimeoutException(WriteType.CAS, cwte.consistency, cwte.received, cwte.blockFor);
                    }
                    break;
                case CAS_WRITE_UNKNOWN:
                    CasWriteUnknownResultException cwue = (CasWriteUnknownResultException) msg.error;
                    return new WriteTimeoutException(WriteType.CAS, cwue.consistency, cwue.received, cwue.blockFor);
            }
        }

        return msg.error;
    }

    // We need to figure error codes out (#3979)
    public final TransportException error;

    private ErrorMessage(TransportException error)
    {
        super(Message.Type.ERROR);
        this.error = error;
    }

    private ErrorMessage(TransportException error, int streamId)
    {
        this(error);
        setStreamId(streamId);
    }

    public static ErrorMessage fromException(Throwable e)
    {
        return fromException(e, null);
    }

    /**
     * @param e the exception
     * @param unexpectedExceptionHandler a callback for handling unexpected exceptions. If null, or if this
     *                                   returns false, the error is logged at ERROR level via sl4fj
     */
    public static ErrorMessage fromException(Throwable e, Predicate<Throwable> unexpectedExceptionHandler)
    {
        int streamId = 0;

        // Netty will wrap exceptions during decoding in a CodecException. If the cause was one of our ProtocolExceptions
        // or some other internal exception, extract that and use it.
        if (e instanceof CodecException)
        {
            Throwable cause = e.getCause();
            if (cause != null)
            {
                if (cause instanceof WrappedException)
                {
                    streamId = ((WrappedException) cause).streamId;
                    e = cause.getCause();
                }
                else if (cause instanceof TransportException)
                {
                    e = cause;
                }
            }
        }
        else if (e instanceof WrappedException)
        {
            streamId = ((WrappedException)e).streamId;
            e = e.getCause();
        }

        if (e instanceof TransportException)
        {
            ErrorMessage message = new ErrorMessage((TransportException) e, streamId);
            if (e instanceof ProtocolException)
            {
                // if the driver attempted to connect with a protocol version not supported then
                // respond with the appropiate version, see ProtocolVersion.decode()
                ProtocolVersion forcedProtocolVersion = ((ProtocolException) e).getForcedProtocolVersion();
                if (forcedProtocolVersion != null)
                    message.forcedProtocolVersion = forcedProtocolVersion;
            }
            return message;
        }

        // Unexpected exception
        if (unexpectedExceptionHandler == null || !unexpectedExceptionHandler.apply(e))
            logger.error("Unexpected exception during request", e);

        return new ErrorMessage(new ServerError(e), streamId);
    }

    @Override
    public String toString()
    {
        return "ERROR " + error.code() + ": " + error.getMessage();
    }

    public static RuntimeException wrap(Throwable t, int streamId)
    {
        return new WrappedException(t, streamId);
    }

    public static class WrappedException extends RuntimeException
    {
        private final int streamId;

        public WrappedException(Throwable cause, int streamId)
        {
            super(cause);
            this.streamId = streamId;
        }

        @VisibleForTesting
        public int getStreamId()
        {
            return this.streamId;
        }
    }

}
