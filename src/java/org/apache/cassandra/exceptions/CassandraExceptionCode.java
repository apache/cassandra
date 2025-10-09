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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * Exception codes for internal CassandraException serialization.
 * Each concrete CassandraException subclass has a unique code to ensure proper type fidelity during serialization.
 * This is separate from the client-facing ExceptionCode enum which maintains backward compatibility.
 */
@Shared(scope = SIMULATION)
public enum CassandraExceptionCode
{
    // Execution exceptions
    UNAVAILABLE                                 (0),   // UnavailableException
    FUNCTION_FAILURE                            (1),   // FunctionExecutionException
    OPERATION_EXECUTION                         (2),   // OperationExecutionException
    OVERLOADED                                  (3),   // OverloadedException
    CAS_WRITE_UNKNOWN                           (4),   // CasWriteUnknownResultException
    IS_BOOTSTRAPPING                            (5),   // IsBootstrappingException
    CDC_WRITE_FAILURE                           (6),   // CDCWriteException
    TRUNCATE_ERROR                              (7),   // TruncateException
    // Timeout exceptions
    READ_TIMEOUT                                (8),   // ReadTimeoutException
    ACCORD_READ_EXHAUSTED                       (9),   // AccordReadExhaustedException
    ACCORD_READ_PREEMPTED                       (10),  // AccordReadPreemptedException
    WRITE_TIMEOUT                               (11),  // WriteTimeoutException
    ACCORD_WRITE_PREEMPTED                      (12),  // AccordWritePreemptedException
    ACCORD_WRITE_EXHAUSTED                      (13),  // AccordWriteExhaustedException
    CAS_WRITE_TIMEOUT                           (14),  // CasWriteTimeoutException
    // Failure exceptions
    READ_FAILURE                                (15),  // ReadFailureException
    TOMBSTONE_ABORT                             (16),  // TombstoneAbortException
    READ_SIZE_ABORT                             (17),  // ReadSizeAbortException
    QUERY_TOO_MANY_INDEXES_ABORT                (18),  // QueryReferencesTooManyIndexesAbortException
    WRITE_FAILURE                               (19),  // WriteFailureException
    // Validation/config exceptions
    UNPREPARED                                  (20),  // PreparedQueryNotFoundException
    BAD_CREDENTIALS                             (21),  // AuthenticationException
    CONFIG_ERROR                                (22),  // ConfigurationException
    ALREADY_EXISTS                              (23),  // AlreadyExistsException
    UNAUTHORIZED                                (24),  // UnauthorizedException
    INVALID                                     (25),  // InvalidRequestException
    INVALID_CONSTRAINT_DEFINITION               (26),  // InvalidConstraintDefinitionException
    CONSTRAINT_VIOLATION                        (27),  // ConstraintViolationException
    OVERSIZED_MESSAGE                           (28),  // OversizedCQLMessageException
    TRIGGER_DISABLED                            (29),  // TriggerDisabledException
    KEYSPACE_NOT_DEFINED                        (30),  // KeyspaceNotDefinedException
    GUARDRAIL_VIOLATED                          (31),  // GuardrailViolatedException
    INVALID_ROUTING                             (32),  // InvalidRoutingException
    MUTATION_EXCEEDED_MAX_SIZE                  (33),  // MutationExceededMaxSizeException
    SYNTAX_ERROR                                (34);  // SyntaxException

    public final int value;
    private static final Map<Integer, CassandraExceptionCode> valueToCode = new HashMap<>(CassandraExceptionCode.values().length);

    static
    {
        for (CassandraExceptionCode code : CassandraExceptionCode.values())
            valueToCode.put(code.value, code);
    }

    CassandraExceptionCode(int value)
    {
        this.value = value;
    }

    public static CassandraExceptionCode fromValue(int value)
    {
        CassandraExceptionCode code = valueToCode.get(value);
        if (code == null)
            throw new IllegalArgumentException(String.format("Unknown CassandraException code %d", value));
        return code;
    }
}