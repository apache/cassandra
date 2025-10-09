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
package org.apache.cassandra.exceptions;

import java.io.IOException;

import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.MutationExceededMaxSizeException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.guardrails.GuardrailViolatedException;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.exceptions.AccordReadExhaustedException;
import org.apache.cassandra.service.accord.exceptions.AccordReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.AccordWriteExhaustedException;
import org.apache.cassandra.service.accord.exceptions.AccordWritePreemptedException;
import org.apache.cassandra.triggers.TriggerDisabledException;
import org.apache.cassandra.utils.ArraySerializers;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION)
public abstract class CassandraException extends RuntimeException implements TransportException
{
    public static final Serializer serializer = new Serializer();

    private final ExceptionCode clientExceptionCode;

    protected CassandraException(ExceptionCode code, String msg)
    {
        super(msg);
        this.clientExceptionCode = code;
    }

    protected CassandraException(ExceptionCode code, String msg, Throwable cause)
    {
        super(msg, cause);
        this.clientExceptionCode = code;
    }

    public ExceptionCode code()
    {
        return clientExceptionCode;
    }

    /**
     * Returns the CassandraExceptionCode for this specific exception class.
     * Each subclass must implement this to return its unique serialization code.
     */
    public abstract CassandraExceptionCode getCassandraExceptionCode();

    /**
     * Serializer for CassandraException that coordinates serialization of subclasses.
     */
    @Shared(scope = SIMULATION)
    public static class Serializer implements IVersionedSerializer<CassandraException>
    {
        @Override
        public void serialize(CassandraException exception, DataOutputPlus out, int version) throws IOException
        {
            String message = exception.getMessage() == null ? "" : exception.getMessage();
            out.writeUnsignedVInt32(exception.getCassandraExceptionCode().value);
            out.writeUTF(message);
            ExceptionSerializer.nullableRemoteExceptionSerializer.serialize(exception.getCause(), out, version);
            ArraySerializers.serializeArray(exception.getSuppressed(), out, version, ExceptionSerializer.remoteExceptionSerializer);
            StackTraceElement[] stackTrace = exception.getStackTrace();
            ArraySerializers.serializeArray(stackTrace, out, version, ExceptionSerializer.stackTraceElementSerializer);
            // delegate to subclass serializer for type-specific fields
            exception.serializeSpecificFields(out, version);
        }

        @Override
        public CassandraException deserialize(DataInputPlus in, int version) throws IOException
        {
            CassandraExceptionCode classCode = CassandraExceptionCode.fromValue(in.readUnsignedVInt32());
            String message = in.readUTF();
            Throwable cause = ExceptionSerializer.nullableRemoteExceptionSerializer.deserialize(in, version);
            Throwable[] suppressed = ArraySerializers.deserializeArray(in, version, ExceptionSerializer.remoteExceptionSerializer, Throwable[]::new);
            StackTraceElement[] stackTrace = ArraySerializers.deserializeArray(in, version, ExceptionSerializer.stackTraceElementSerializer, StackTraceElement[]::new);
            // delegate to subclass serializer for type-specific fields
            CassandraException exception = deserializeByClassCode(classCode, message, in, version);

            if (cause != null)
                exception.initCause(cause);
            for (Throwable t : suppressed)
                exception.addSuppressed(t);
            exception.setStackTrace(stackTrace);

            return exception;
        }

        @Override
        public long serializedSize(CassandraException exception, int version)
        {
            String message = exception.getMessage() == null ? "" : exception.getMessage();
            long size = TypeSizes.sizeofUnsignedVInt(exception.getCassandraExceptionCode().value);
            size += TypeSizes.sizeof(message);
            size += ExceptionSerializer.nullableRemoteExceptionSerializer.serializedSize(exception.getCause(), version);
            size += ArraySerializers.serializedArraySize(exception.getSuppressed(), version, ExceptionSerializer.remoteExceptionSerializer);
            size += ArraySerializers.serializedArraySize(exception.getStackTrace(), version, ExceptionSerializer.stackTraceElementSerializer);
            // delegate to subclass serializer for type-specific fields
            size += exception.serializedSizeSpecificFields(version);
            return size;
        }

        private CassandraException deserializeByClassCode(CassandraExceptionCode cassandraExceptionCode, String message, DataInputPlus in, int version) throws IOException
        {
            // Create the appropriate exception instance based on the class code
            switch (cassandraExceptionCode)
            {
                case UNAVAILABLE:
                    return UnavailableException.deserializeFields(message, in, version);
                case FUNCTION_FAILURE:
                    return FunctionExecutionException.deserializeFields(message, in, version);
                case OPERATION_EXECUTION:
                    return OperationExecutionException.deserializeFields(message, in, version);
                case OVERLOADED:
                    return new OverloadedException(message);
                case CAS_WRITE_UNKNOWN:
                    return CasWriteUnknownResultException.deserializeFields(message, in, version);
                case IS_BOOTSTRAPPING:
                    return new IsBootstrappingException();
                case CDC_WRITE_FAILURE:
                    return new CDCWriteException(message);
                case TRUNCATE_ERROR:
                    return new TruncateException(message);
                case READ_TIMEOUT:
                    return ReadTimeoutException.deserializeFields(message, in, version);
                case ACCORD_READ_EXHAUSTED:
                    return AccordReadExhaustedException.deserializeFields(message, in, version);
                case ACCORD_READ_PREEMPTED:
                    return AccordReadPreemptedException.deserializeFields(message, in, version);
                case ACCORD_WRITE_PREEMPTED:
                    return AccordWritePreemptedException.deserializeFields(message, in, version);
                case ACCORD_WRITE_EXHAUSTED:
                    return AccordWriteExhaustedException.deserializeFields(message, in, version);
                case WRITE_TIMEOUT:
                    return WriteTimeoutException.deserializeFields(message, in, version);
                case CAS_WRITE_TIMEOUT:
                    return CasWriteTimeoutException.deserializeFields(message, in, version);
                case READ_FAILURE:
                    return ReadFailureException.deserializeFields(message, in, version);
                case TOMBSTONE_ABORT:
                    return TombstoneAbortException.deserializeFields(message, in, version);
                case READ_SIZE_ABORT:
                    return ReadSizeAbortException.deserializeFields(message, in, version);
                case QUERY_TOO_MANY_INDEXES_ABORT:
                    return QueryReferencesTooManyIndexesAbortException.deserializeFields(message, in, version);
                case WRITE_FAILURE:
                    return WriteFailureException.deserializeFields(message, in, version);
                case UNPREPARED:
                    return PreparedQueryNotFoundException.deserializeFields(message, in, version);
                case BAD_CREDENTIALS:
                    return new AuthenticationException(message);
                case CONFIG_ERROR:
                    return new ConfigurationException(message);
                case ALREADY_EXISTS:
                    return AlreadyExistsException.deserializeFields(message, in, version);
                case UNAUTHORIZED:
                    return new UnauthorizedException(message);
                case INVALID:
                    return new InvalidRequestException(message);
                case INVALID_CONSTRAINT_DEFINITION:
                    return new InvalidConstraintDefinitionException(message);
                case CONSTRAINT_VIOLATION:
                    return new ConstraintViolationException(message);
                case TRIGGER_DISABLED:
                    return new TriggerDisabledException(message);
                case KEYSPACE_NOT_DEFINED:
                    return new KeyspaceNotDefinedException(message);
                case GUARDRAIL_VIOLATED:
                    return new GuardrailViolatedException(message);
                case MUTATION_EXCEEDED_MAX_SIZE:
                    return MutationExceededMaxSizeException.deserializeFields(message, in, version);
                case OVERSIZED_MESSAGE:
                    return new OversizedCQLMessageException(message);
                case INVALID_ROUTING:
                    return new InvalidRoutingException(message);
                case SYNTAX_ERROR:
                    return new SyntaxException(message);
                default:
                    throw new AssertionError("Unhandled CassandraExceptionCode: " + cassandraExceptionCode);
            }
        }
    }

    /**
     * Serialize subclass-specific fields. Override in subclasses that have additional fields.
     */
    protected void serializeSpecificFields(DataOutputPlus out, int version) throws IOException
    {
    }

    /**
     * Calculate serialized size of subclass-specific fields. Override in subclasses that have additional fields.
     */
    protected long serializedSizeSpecificFields(int version)
    {
        return 0;
    }
}
