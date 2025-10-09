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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.cql3.constraints.ConstraintViolationException;
import org.apache.cassandra.cql3.constraints.InvalidConstraintDefinitionException;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.KeyspaceNotDefinedException;
import org.apache.cassandra.db.MutationExceededMaxSizeException;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.guardrails.GuardrailViolatedException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.exceptions.AccordReadExhaustedException;
import org.apache.cassandra.service.accord.exceptions.AccordReadPreemptedException;
import org.apache.cassandra.service.accord.exceptions.AccordWriteExhaustedException;
import org.apache.cassandra.service.accord.exceptions.AccordWritePreemptedException;
import org.apache.cassandra.triggers.TriggerDisabledException;
import org.apache.cassandra.utils.MD5Digest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Comprehensive tests for CassandraException serialization.
 * Tests that all exception types properly serialize/deserialize with:
 * - Correct type fidelity (no type loss during roundtrip)
 * - All exception-specific fields preserved
 * - Stack traces, causes, and suppressed exceptions preserved
 */
public class CassandraExceptionSerializationTest
{
    private static final int VERSION = MessagingService.current_version;

    @Test
    public void testUnavailableException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.QUORUM;
        int required = 3;
        int alive = 1;

        UnavailableException original = UnavailableException.create(cl, required, alive);

        // Add cause and stack trace
        RuntimeException cause = new RuntimeException("Test cause");
        original.initCause(cause);
        original.addSuppressed(new IllegalArgumentException("Suppressed exception"));

        UnavailableException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(UnavailableException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(required, deserialized.required);
        assertEquals(alive, deserialized.alive);
        assertNotNull(deserialized.getMessage());

        // Verify cause and suppressed exceptions are preserved (now as RemoteExceptions with host prefix)
        assertNotNull(deserialized.getCause());
        assertTrue("Cause should contain 'Test cause'", deserialized.getCause().getMessage().contains("Test cause"));
        assertEquals(1, deserialized.getSuppressed().length);
        assertTrue("Suppressed should contain 'Suppressed exception'",
                  deserialized.getSuppressed()[0].getMessage().contains("Suppressed exception"));

        // Verify stack trace is preserved
        assertTrue(deserialized.getStackTrace().length > 0);
    }

    @Test
    public void testWriteFailureException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.QUORUM;
        int received = 2;
        int blockFor = 3;
        WriteType writeType = WriteType.SIMPLE;
        Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();
        InetAddressAndPort addr1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort addr2 = InetAddressAndPort.getByName("127.0.0.2");
        failures.put(addr1, RequestFailureReason.TIMEOUT);
        failures.put(addr2, RequestFailureReason.UNKNOWN);

        WriteFailureException original = new WriteFailureException(cl, received, blockFor, writeType, failures);

        WriteFailureException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(WriteFailureException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(writeType, deserialized.writeType);
        assertEquals(failures.size(), deserialized.failureReasonByEndpoint.size());
        assertEquals(failures.get(addr1), deserialized.failureReasonByEndpoint.get(addr1));
        assertEquals(failures.get(addr2), deserialized.failureReasonByEndpoint.get(addr2));
    }

    @Test
    public void testReadFailureException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.QUORUM;
        int received = 2;
        int blockFor = 3;
        boolean dataPresent = true;
        Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        failures.put(addr, RequestFailureReason.TIMEOUT);

        ReadFailureException original = new ReadFailureException(cl, received, blockFor, dataPresent, failures);

        ReadFailureException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(ReadFailureException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(dataPresent, deserialized.dataPresent);
        assertEquals(failures.size(), deserialized.failureReasonByEndpoint.size());
        assertEquals(failures.get(addr), deserialized.failureReasonByEndpoint.get(addr));
    }

    @Test
    public void testTombstoneAbortException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.ONE;
        int received = 1;
        int blockFor = 1;
        boolean dataPresent = true;
        int nodes = 3;
        long tombstones = 100000L;
        Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        failures.put(addr, RequestFailureReason.READ_TOO_MANY_TOMBSTONES);

        TombstoneAbortException original = new TombstoneAbortException(
            "Too many tombstones", nodes, tombstones, dataPresent, cl, received, blockFor, failures);

        TombstoneAbortException deserialized = roundTrip(original);

        // Verify type fidelity - this should now preserve the exact type
        assertEquals(TombstoneAbortException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(dataPresent, deserialized.dataPresent);
        assertEquals(nodes, deserialized.nodes);
        assertEquals(tombstones, deserialized.tombstones);
        assertEquals(failures.size(), deserialized.failureReasonByEndpoint.size());
        assertEquals(failures.get(addr), deserialized.failureReasonByEndpoint.get(addr));
    }

    @Test
    public void testReadSizeAbortException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.LOCAL_ONE;
        int received = 1;
        int blockFor = 1;
        boolean dataPresent = true;
        Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        failures.put(addr, RequestFailureReason.READ_SIZE);

        ReadSizeAbortException original = new ReadSizeAbortException(
            "Read size too large", cl, received, blockFor, dataPresent, failures);

        ReadSizeAbortException deserialized = roundTrip(original);

        // Verify type fidelity - this should now preserve the exact type
        assertEquals(ReadSizeAbortException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(dataPresent, deserialized.dataPresent);
        assertEquals(failures.size(), deserialized.failureReasonByEndpoint.size());
        assertEquals(failures.get(addr), deserialized.failureReasonByEndpoint.get(addr));
    }

    @Test
    public void testQueryReferencesTooManyIndexesAbortException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.QUORUM;
        int received = 2;
        int blockFor = 2;
        boolean dataPresent = true;
        int nodes = 2;
        long maxValue = 5L;
        Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        failures.put(addr, RequestFailureReason.INDEX_NOT_AVAILABLE);

        QueryReferencesTooManyIndexesAbortException original = new QueryReferencesTooManyIndexesAbortException(
            "Too many indexes", nodes, maxValue, dataPresent, cl, received, blockFor, failures);

        QueryReferencesTooManyIndexesAbortException deserialized = roundTrip(original);

        // Verify type fidelity - this should now preserve the exact type
        assertEquals(QueryReferencesTooManyIndexesAbortException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(dataPresent, deserialized.dataPresent);
        assertEquals(nodes, deserialized.nodes);
        assertEquals(maxValue, deserialized.maxValue);
        assertEquals(failures.size(), deserialized.failureReasonByEndpoint.size());
        assertEquals(failures.get(addr), deserialized.failureReasonByEndpoint.get(addr));
    }

    @Test
    public void testOverloadedException() throws IOException
    {
        String message = "Server overloaded";
        OverloadedException original = new OverloadedException(message);

        OverloadedException deserialized = roundTrip(original);

        // Verify type fidelity - this should now preserve the exact type instead of becoming InvalidRequestException
        assertEquals(OverloadedException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testTruncateException() throws IOException
    {
        String message = "Truncate failed due to timeout";
        TruncateException original = new TruncateException(message);

        TruncateException deserialized = roundTrip(original);

        // Verify type fidelity - this should now preserve the exact type instead of becoming InvalidRequestException
        assertEquals(TruncateException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testAuthenticationException() throws IOException
    {
        String message = "Authentication failed";
        AuthenticationException original = new AuthenticationException(message);

        AuthenticationException deserialized = roundTrip(original);

        // Verify type fidelity - this should now preserve the exact type instead of becoming InvalidRequestException
        assertEquals(AuthenticationException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testFunctionExecutionException() throws IOException
    {
        FunctionName functionName = new FunctionName("test_ks", "test_fn");
        List<String> argTypes = Arrays.asList("int", "text");
        String detail = "division by zero";

        FunctionExecutionException original = FunctionExecutionException.create(functionName, argTypes, detail);
        FunctionExecutionException deserialized = roundTrip(original);

        assertEquals(FunctionExecutionException.class, deserialized.getClass());
        assertEquals(functionName.keyspace, deserialized.functionName.keyspace);
        assertEquals(functionName.name, deserialized.functionName.name);
        assertEquals(argTypes, deserialized.argTypes);
        assertEquals(original.detail, deserialized.detail);
    }

    @Test
    public void testOperationExecutionException() throws IOException
    {
        List<String> argTypes = Arrays.asList("int", "int");
        String detail = "Division by zero";

        OperationExecutionException original = new OperationExecutionException('/', argTypes, detail);

        OperationExecutionException deserialized = roundTrip(original);

        // Verify type fidelity - should remain OperationExecutionException, not become FunctionExecutionException
        assertEquals(OperationExecutionException.class, deserialized.getClass());

        // '/' operator maps to function name "_divide"
        assertEquals("_divide", deserialized.functionName.name);
        assertEquals(argTypes, deserialized.argTypes);
        assertTrue(deserialized.detail.contains(detail));
    }

    @Test
    public void testNestedExceptionSerialization() throws IOException
    {
        // Create a complex nested exception scenario
        InvalidRequestException original = new InvalidRequestException("Root error");

        // Add cause chain
        RuntimeException cause1 = new RuntimeException("Cause level 1");
        IllegalArgumentException cause2 = new IllegalArgumentException("Cause level 2");
        cause1.initCause(cause2);
        original.initCause(cause1);

        // Add suppressed exceptions
        original.addSuppressed(new IllegalStateException("Suppressed 1"));
        original.addSuppressed(new UnsupportedOperationException("Suppressed 2"));

        // Set stack trace
        StackTraceElement[] stackTrace = {
            new StackTraceElement("TestClass", "testMethod", "TestClass.java", 100),
            new StackTraceElement("AnotherClass", "anotherMethod", "AnotherClass.java", 200)
        };
        original.setStackTrace(stackTrace);

        InvalidRequestException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals("Root error", deserialized.getMessage());

        // Verify cause chain (now using RemoteException format)
        assertNotNull(deserialized.getCause());
        assertTrue("Cause should contain 'Cause level 1'", deserialized.getCause().getMessage().contains("Cause level 1"));
        assertNotNull(deserialized.getCause().getCause());
        assertTrue("Nested cause should contain 'Cause level 2'", deserialized.getCause().getCause().getMessage().contains("Cause level 2"));

        // Verify suppressed exceptions (now using RemoteException format)
        assertEquals(2, deserialized.getSuppressed().length);
        assertTrue("Suppressed 1 should contain message", deserialized.getSuppressed()[0].getMessage().contains("Suppressed 1"));
        assertTrue("Suppressed 2 should contain message", deserialized.getSuppressed()[1].getMessage().contains("Suppressed 2"));

        // Verify stack trace
        assertEquals(2, deserialized.getStackTrace().length);
        assertEquals("TestClass", deserialized.getStackTrace()[0].getClassName());
        assertEquals("testMethod", deserialized.getStackTrace()[0].getMethodName());
        assertEquals("TestClass.java", deserialized.getStackTrace()[0].getFileName());
        assertEquals(100, deserialized.getStackTrace()[0].getLineNumber());
    }

    @Test
    public void testReadTimeoutException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.QUORUM;
        int received = 2;
        int blockFor = 3;
        boolean dataPresent = true;

        ReadTimeoutException original = new ReadTimeoutException(cl, received, blockFor, dataPresent);

        ReadTimeoutException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(ReadTimeoutException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(dataPresent, deserialized.dataPresent);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testWriteTimeoutException() throws IOException
    {
        WriteType writeType = WriteType.SIMPLE;
        ConsistencyLevel cl = ConsistencyLevel.LOCAL_QUORUM;
        int received = 1;
        int blockFor = 2;

        WriteTimeoutException original = new WriteTimeoutException(writeType, cl, received, blockFor);

        WriteTimeoutException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(WriteTimeoutException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(writeType, deserialized.writeType);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testCasWriteTimeoutException() throws IOException
    {
        WriteType writeType = WriteType.CAS;
        ConsistencyLevel cl = ConsistencyLevel.SERIAL;
        int received = 1;
        int blockFor = 3;
        int contentions = 5;

        CasWriteTimeoutException original = new CasWriteTimeoutException(writeType, cl, received, blockFor, contentions);

        CasWriteTimeoutException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(CasWriteTimeoutException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(writeType, deserialized.writeType);
        assertEquals(contentions, deserialized.contentions);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testAccordReadExhaustedException() throws IOException
    {
        int received = 1;
        int blockFor = 2;
        boolean dataPresent = false;

        AccordReadExhaustedException original = new AccordReadExhaustedException(received, blockFor, dataPresent);

        AccordReadExhaustedException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(AccordReadExhaustedException.class, deserialized.getClass());

        // Verify specific fields (these extend ReadTimeoutException)
        assertEquals(ConsistencyLevel.SERIAL, deserialized.consistency); // Accord exceptions use SERIAL
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(dataPresent, deserialized.dataPresent);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testAccordReadPreemptedException() throws IOException
    {
        int received = 0;
        int blockFor = 3;
        boolean dataPresent = true;

        AccordReadPreemptedException original = new AccordReadPreemptedException(received, blockFor, dataPresent);

        AccordReadPreemptedException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(AccordReadPreemptedException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(ConsistencyLevel.SERIAL, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(dataPresent, deserialized.dataPresent);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testAccordWriteExhaustedException() throws IOException
    {
        int received = 1;
        int blockFor = 2;

        AccordWriteExhaustedException original = new AccordWriteExhaustedException(received, blockFor);

        AccordWriteExhaustedException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(AccordWriteExhaustedException.class, deserialized.getClass());

        // Verify specific fields (these extend WriteTimeoutException)
        assertEquals(ConsistencyLevel.SERIAL, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testAccordWritePreemptedException() throws IOException
    {
        int received = 0;
        int blockFor = 3;

        AccordWritePreemptedException original = new AccordWritePreemptedException(received, blockFor);

        AccordWritePreemptedException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(AccordWritePreemptedException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(ConsistencyLevel.SERIAL, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testConstraintViolationException() throws IOException
    {
        String message = "Constraint violated: value out of range";
        ConstraintViolationException original = new ConstraintViolationException(message);

        ConstraintViolationException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(ConstraintViolationException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testInvalidConstraintDefinitionException() throws IOException
    {
        String message = "Invalid constraint definition: syntax error";
        InvalidConstraintDefinitionException original = new InvalidConstraintDefinitionException(message);

        InvalidConstraintDefinitionException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(InvalidConstraintDefinitionException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testTriggerDisabledException() throws IOException
    {
        String message = "Trigger is disabled";
        TriggerDisabledException original = new TriggerDisabledException(message);

        TriggerDisabledException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(TriggerDisabledException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testKeyspaceNotDefinedException() throws IOException
    {
        String message = "Keyspace 'test_ks' does not exist";
        KeyspaceNotDefinedException original = new KeyspaceNotDefinedException(message);

        KeyspaceNotDefinedException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(KeyspaceNotDefinedException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testGuardrailViolatedException() throws IOException
    {
        String message = "Query exceeded guardrail: too many tombstones";
        GuardrailViolatedException original = new GuardrailViolatedException(message);

        GuardrailViolatedException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(GuardrailViolatedException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
    }

    @Test
    public void testMutationExceededMaxSizeException() throws IOException
    {
        String message = "Mutation size exceeded maximum: 16MB/12MB for keyspace test_ks";
        long mutationSize = 16777216L; // 16MB

        MutationExceededMaxSizeException original = new MutationExceededMaxSizeException(message, mutationSize);

        MutationExceededMaxSizeException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(MutationExceededMaxSizeException.class, deserialized.getClass());
        assertEquals(message, deserialized.getMessage());
        assertEquals(mutationSize, deserialized.mutationSize);
    }

    @Test
    public void testAllSimpleExceptionTypes() throws IOException
    {
        // Test all simple exception types that should roundtrip correctly
        testSimpleException(new SyntaxException("Syntax error"));
        testSimpleException(new UnauthorizedException("Not authorized"));
        testSimpleException(new InvalidRequestException("Invalid request"));
        testSimpleException(new ConfigurationException("Config error"));
        testSimpleException(new CDCWriteException("CDC write failed"));
        testSimpleException(new IsBootstrappingException());
        testSimpleException(new OversizedCQLMessageException("Message too large"));
        testSimpleException(new InvalidRoutingException("Invalid routing"));
    }

    private void testSimpleException(CassandraException original) throws IOException
    {
        CassandraException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals("Type should be preserved for " + original.getClass().getSimpleName(),
                    original.getClass(), deserialized.getClass());

        // Verify message preservation
        assertEquals("Message should be preserved for " + original.getClass().getSimpleName(),
                    original.getMessage(), deserialized.getMessage());

        // Verify legacy code is preserved for protocol compatibility
        assertEquals("Legacy ExceptionCode should be preserved for " + original.getClass().getSimpleName(),
                    original.code(), deserialized.code());
    }

    /**
     * Helper method to serialize and deserialize an exception
     */
    @SuppressWarnings("unchecked")
    private <T extends CassandraException> T roundTrip(T original) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        CassandraException.serializer.serialize(original, out, VERSION);

        // Verify serialized size matches actual size
        long expectedSize = CassandraException.serializer.serializedSize(original, VERSION);
        assertEquals("Serialized size mismatch for " + original.getClass().getSimpleName(),
                    expectedSize, out.getLength());

        DataInputBuffer in = new DataInputBuffer(out.toByteArray());
        return (T) CassandraException.serializer.deserialize(in, VERSION);
    }

    // Edge Case Tests

    @Test
    public void testNullAndEmptyMessages() throws IOException
    {
        // Test null message
        InvalidRequestException nullMessage = new InvalidRequestException(null);
        InvalidRequestException deserializedNull = roundTrip(nullMessage);
        assertEquals(InvalidRequestException.class, deserializedNull.getClass());
        // Null messages get converted to empty strings in serialization
        assertEquals("", deserializedNull.getMessage());

        // Test empty message
        InvalidRequestException emptyMessage = new InvalidRequestException("");
        InvalidRequestException deserializedEmpty = roundTrip(emptyMessage);
        assertEquals(InvalidRequestException.class, deserializedEmpty.getClass());
        assertEquals("", deserializedEmpty.getMessage());
    }

    @Test
    public void testUnicodeMessages() throws IOException
    {
        // Test various Unicode characters
        String unicodeMessage = "Error: 中文测试 🔥 Ω α β γ 🚀 Ñoël";
        InvalidRequestException original = new InvalidRequestException(unicodeMessage);

        InvalidRequestException deserialized = roundTrip(original);

        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals(unicodeMessage, deserialized.getMessage());
    }

    @Test
    public void testVeryLargeMessages() throws IOException
    {
        // Test large message (16KB - within TypeSizes limits)
        StringBuilder largeMessage = new StringBuilder();
        for (int i = 0; i < 16384; i++)
        {
            largeMessage.append("A");
        }

        InvalidRequestException original = new InvalidRequestException(largeMessage.toString());
        InvalidRequestException deserialized = roundTrip(original);

        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals(largeMessage.toString(), deserialized.getMessage());
    }

    @Test
    public void testExceptionWithNullStackTrace() throws IOException
    {
        // Note: Java doesn't allow setting null stack trace, so we test empty instead
        InvalidRequestException original = new InvalidRequestException("Test message");
        original.setStackTrace(new StackTraceElement[0]);

        InvalidRequestException deserialized = roundTrip(original);

        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals("Test message", deserialized.getMessage());
        // Empty stack trace should be preserved
        assertEquals(0, deserialized.getStackTrace().length);
    }

    @Test
    public void testStackTraceWithNullElements() throws IOException
    {
        InvalidRequestException original = new InvalidRequestException("Test message");

        // Stack trace with null file name and negative line number
        StackTraceElement[] stackTrace = {
            new StackTraceElement("TestClass", "testMethod", null, -1),
            new StackTraceElement("AnotherClass", "anotherMethod", "AnotherClass.java", 0)
        };
        original.setStackTrace(stackTrace);

        InvalidRequestException deserialized = roundTrip(original);

        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals(2, deserialized.getStackTrace().length);

        StackTraceElement elem1 = deserialized.getStackTrace()[0];
        assertEquals("TestClass", elem1.getClassName());
        assertEquals("testMethod", elem1.getMethodName());
        assertNull(elem1.getFileName());
        assertEquals(-1, elem1.getLineNumber());
    }

    @Test
    public void testExceptionWithoutCauseOrSuppressed() throws IOException
    {
        InvalidRequestException original = new InvalidRequestException("Test message");
        // Explicitly ensure no cause or suppressed exceptions

        InvalidRequestException deserialized = roundTrip(original);

        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals("Test message", deserialized.getMessage());
        assertNull(deserialized.getCause());
        assertEquals(0, deserialized.getSuppressed().length);
    }

    @Test
    public void testUnavailableExceptionEdgeCases() throws IOException
    {
        // Test with zero alive, one required
        UnavailableException zeroValues = UnavailableException.create(ConsistencyLevel.ONE, 1, 0);
        UnavailableException deserializedZero = roundTrip(zeroValues);

        assertEquals(UnavailableException.class, deserializedZero.getClass());
        assertEquals(ConsistencyLevel.ONE, deserializedZero.consistency);
        assertEquals(1, deserializedZero.required);
        assertEquals(0, deserializedZero.alive);

        // Test with very large values
        UnavailableException largeValues = UnavailableException.create(ConsistencyLevel.ALL, Integer.MAX_VALUE, Integer.MAX_VALUE - 1);
        UnavailableException deserializedLarge = roundTrip(largeValues);

        assertEquals(UnavailableException.class, deserializedLarge.getClass());
        assertEquals(ConsistencyLevel.ALL, deserializedLarge.consistency);
        assertEquals(Integer.MAX_VALUE, deserializedLarge.required);
        assertEquals(Integer.MAX_VALUE - 1, deserializedLarge.alive);
    }

    @Test
    public void testWriteFailureExceptionWithEmptyFailuresMap() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.ONE;
        int received = 1;
        int blockFor = 1;
        WriteType writeType = WriteType.SIMPLE;
        Map<InetAddressAndPort, RequestFailureReason> emptyFailures = new HashMap<>();

        WriteFailureException original = new WriteFailureException(cl, received, blockFor, writeType, emptyFailures);
        WriteFailureException deserialized = roundTrip(original);

        assertEquals(WriteFailureException.class, deserialized.getClass());
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertEquals(writeType, deserialized.writeType);
        assertEquals(0, deserialized.failureReasonByEndpoint.size());
    }

    @Test
    public void testTombstoneAbortExceptionExtremValues() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.ONE;
        int received = 1;
        int blockFor = 1;
        boolean dataPresent = true;

        // Test with extreme values
        int maxNodes = Integer.MAX_VALUE;
        long maxTombstones = Long.MAX_VALUE;
        Map<InetAddressAndPort, RequestFailureReason> failures = new HashMap<>();

        TombstoneAbortException original = new TombstoneAbortException(
            "Max values test", maxNodes, maxTombstones, dataPresent, cl, received, blockFor, failures);

        TombstoneAbortException deserialized = roundTrip(original);

        assertEquals(TombstoneAbortException.class, deserialized.getClass());
        assertEquals(maxNodes, deserialized.nodes);
        assertEquals(maxTombstones, deserialized.tombstones);

        // Test with zero/negative values
        TombstoneAbortException zeroValues = new TombstoneAbortException(
            "Zero values test", 0, 0L, dataPresent, cl, received, blockFor, failures);

        TombstoneAbortException deserializedZero = roundTrip(zeroValues);

        assertEquals(TombstoneAbortException.class, deserializedZero.getClass());
        assertEquals(0, deserializedZero.nodes);
        assertEquals(0L, deserializedZero.tombstones);
    }

    @Test
    public void testFunctionExecutionExceptionWithNullKeyspace() throws IOException
    {
        FunctionName functionNameNull = new FunctionName(null, "system_function");
        List<String> argTypes = Arrays.asList("text", "int");
        String detail = "System function error";

        FunctionExecutionException original = new FunctionExecutionException(functionNameNull, argTypes, detail);
        FunctionExecutionException deserialized = roundTrip(original);

        assertEquals(FunctionExecutionException.class, deserialized.getClass());
        assertNull(deserialized.functionName.keyspace);
        assertEquals("system_function", deserialized.functionName.name);
        assertEquals(argTypes, deserialized.argTypes);
        assertEquals(detail, deserialized.detail);
    }

    @Test
    public void testFunctionExecutionExceptionWithEmptyArgTypes() throws IOException
    {
        FunctionName functionName = new FunctionName("test_ks", "no_arg_function");
        List<String> emptyArgTypes = Arrays.asList();
        String detail = "No arguments function";

        FunctionExecutionException original = new FunctionExecutionException(functionName, emptyArgTypes, detail);
        FunctionExecutionException deserialized = roundTrip(original);

        assertEquals(FunctionExecutionException.class, deserialized.getClass());
        assertEquals("test_ks", deserialized.functionName.keyspace);
        assertEquals("no_arg_function", deserialized.functionName.name);
        assertEquals(0, deserialized.argTypes.size());
        assertEquals(detail, deserialized.detail);
    }

    @Test
    public void testPreparedQueryNotFoundExceptionWithZeroId() throws IOException
    {
        byte[] zeroBytes = new byte[16];
        // All zeros
        MD5Digest zeroId = MD5Digest.wrap(zeroBytes);

        PreparedQueryNotFoundException original = new PreparedQueryNotFoundException(zeroId);
        PreparedQueryNotFoundException deserialized = roundTrip(original);

        assertArrayEquals(zeroId.bytes, deserialized.id.bytes);
        assertNotNull(deserialized.getMessage());
        assertTrue(deserialized.getMessage().contains(zeroId.toString()));
    }

    @Test
    public void testAlreadyExistsExceptionWithEmptyNames() throws IOException
    {
        // Test with empty keyspace name
        AlreadyExistsException emptyKs = new AlreadyExistsException("", "table");
        AlreadyExistsException deserializedEmptyKs = roundTrip(emptyKs);

        assertEquals(AlreadyExistsException.class, deserializedEmptyKs.getClass());
        assertEquals("", deserializedEmptyKs.ksName);
        assertEquals("table", deserializedEmptyKs.cfName);

        // Test with empty table name
        AlreadyExistsException emptyTable = new AlreadyExistsException("keyspace", "");
        AlreadyExistsException deserializedEmptyTable = roundTrip(emptyTable);

        assertEquals(AlreadyExistsException.class, deserializedEmptyTable.getClass());
        assertEquals("keyspace", deserializedEmptyTable.ksName);
        assertEquals("", deserializedEmptyTable.cfName);
    }

    @Test
    public void testCasWriteUnknownResultExceptionEdgeCases() throws IOException
    {
        // Test with zero values
        CasWriteUnknownResultException zeroValues = new CasWriteUnknownResultException(
            ConsistencyLevel.SERIAL, 0, 0);
        CasWriteUnknownResultException deserializedZero = roundTrip(zeroValues);

        assertEquals(CasWriteUnknownResultException.class, deserializedZero.getClass());
        assertEquals(ConsistencyLevel.SERIAL, deserializedZero.consistency);
        assertEquals(0, deserializedZero.received);
        assertEquals(0, deserializedZero.blockFor);

        // Test with maximum values
        CasWriteUnknownResultException maxValues = new CasWriteUnknownResultException(
            ConsistencyLevel.LOCAL_SERIAL, Integer.MAX_VALUE, Integer.MAX_VALUE);
        CasWriteUnknownResultException deserializedMax = roundTrip(maxValues);

        assertEquals(CasWriteUnknownResultException.class, deserializedMax.getClass());
        assertEquals(ConsistencyLevel.LOCAL_SERIAL, deserializedMax.consistency);
        assertEquals(Integer.MAX_VALUE, deserializedMax.received);
        assertEquals(Integer.MAX_VALUE, deserializedMax.blockFor);
    }

    @Test
    public void testDeepNestedCauseChain() throws IOException
    {
        // Create a deep cause chain
        InvalidRequestException root = new InvalidRequestException("Root exception");

        Throwable current = root;
        for (int i = 1; i <= 10; i++)
        {
            RuntimeException cause = new RuntimeException("Cause level " + i);
            current.initCause(cause);
            current = cause;
        }

        InvalidRequestException deserialized = roundTrip(root);

        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals("Root exception", deserialized.getMessage());

        // Verify the deep cause chain is preserved (now using RemoteException format)
        Throwable currentDeserialized = deserialized.getCause();
        for (int i = 1; i <= 10; i++)
        {
            assertNotNull("Cause level " + i + " should not be null", currentDeserialized);
            assertTrue("Cause level " + i + " should contain expected message",
                      currentDeserialized.getMessage().contains("Cause level " + i));
            currentDeserialized = currentDeserialized.getCause();
        }
        assertNull("Should not have more causes", currentDeserialized);
    }

    @Test
    public void testManySuppressedExceptions() throws IOException
    {
        InvalidRequestException original = new InvalidRequestException("Root with many suppressed");

        // Add many suppressed exceptions
        for (int i = 0; i < 50; i++)
        {
            original.addSuppressed(new IllegalArgumentException("Suppressed " + i));
        }

        InvalidRequestException deserialized = roundTrip(original);

        assertEquals(InvalidRequestException.class, deserialized.getClass());
        assertEquals("Root with many suppressed", deserialized.getMessage());
        assertEquals(50, deserialized.getSuppressed().length);

        for (int i = 0; i < 50; i++)
        {
            assertTrue("Suppressed " + i + " should contain expected message",
                      deserialized.getSuppressed()[i].getMessage().contains("Suppressed " + i));
        }
    }

    @Test
    public void testCasWriteUnknownResultException() throws IOException
    {
        ConsistencyLevel cl = ConsistencyLevel.SERIAL;
        int received = 2;
        int blockFor = 3;

        CasWriteUnknownResultException original = new CasWriteUnknownResultException(cl, received, blockFor);

        CasWriteUnknownResultException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(CasWriteUnknownResultException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(cl, deserialized.consistency);
        assertEquals(received, deserialized.received);
        assertEquals(blockFor, deserialized.blockFor);
        assertNotNull(deserialized.getMessage());
    }

    @Test
    public void testPreparedQueryNotFoundException() throws IOException
    {
        byte[] idBytes = {0x01, 0x23, 0x45, 0x67, (byte)0x89, (byte)0xAB, (byte)0xCD, (byte)0xEF,
                         0x01, 0x23, 0x45, 0x67, (byte)0x89, (byte)0xAB, (byte)0xCD, (byte)0xEF};
        MD5Digest id = MD5Digest.wrap(idBytes);

        PreparedQueryNotFoundException original = new PreparedQueryNotFoundException(id);

        PreparedQueryNotFoundException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(PreparedQueryNotFoundException.class, deserialized.getClass());

        // Verify specific fields
        assertArrayEquals(id.bytes, deserialized.id.bytes);
        assertNotNull(deserialized.getMessage());
        assertTrue(deserialized.getMessage().contains(id.toString()));
    }

    @Test
    public void testAlreadyExistsException() throws IOException
    {
        String ksName = "test_keyspace";
        String cfName = "test_table";

        AlreadyExistsException original = new AlreadyExistsException(ksName, cfName);

        AlreadyExistsException deserialized = roundTrip(original);

        // Verify type fidelity
        assertEquals(AlreadyExistsException.class, deserialized.getClass());

        // Verify specific fields
        assertEquals(ksName, deserialized.ksName);
        assertEquals(cfName, deserialized.cfName);
        assertNotNull(deserialized.getMessage());
        assertTrue(deserialized.getMessage().contains(ksName));
        assertTrue(deserialized.getMessage().contains(cfName));
    }

    @Test
    public void testSerializedSizeAccuracy() throws IOException
    {
        // Test that serialized size calculations are accurate for various exception types

        // Simple exception
        InvalidRequestException simple = new InvalidRequestException("Simple message");
        verifySerializedSizeAccuracy(simple);

        // Exception with cause and suppressed
        InvalidRequestException complex = new InvalidRequestException("Complex message");
        complex.initCause(new RuntimeException("Cause"));
        complex.addSuppressed(new IllegalStateException("Suppressed"));
        verifySerializedSizeAccuracy(complex);

        // Exception with custom stack trace
        InvalidRequestException withStack = new InvalidRequestException("With stack");
        withStack.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("Class1", "method1", "File1.java", 100),
            new StackTraceElement("Class2", "method2", null, -1)
        });
        verifySerializedSizeAccuracy(withStack);

        // Complex exception with fields
        UnavailableException unavailable = UnavailableException.create(ConsistencyLevel.QUORUM, 5, 2);
        verifySerializedSizeAccuracy(unavailable);

        // Exception with large data
        Map<InetAddressAndPort, RequestFailureReason> largeFailures = new HashMap<>();
        for (int i = 1; i <= 100; i++)
        {
            largeFailures.put(InetAddressAndPort.getByName("192.168.1." + i), RequestFailureReason.TIMEOUT);
        }
        WriteFailureException largeFailure = new WriteFailureException(
            ConsistencyLevel.ALL, 50, 100, WriteType.BATCH, largeFailures);
        verifySerializedSizeAccuracy(largeFailure);
    }

    private void verifySerializedSizeAccuracy(CassandraException exception) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        CassandraException.serializer.serialize(exception, out, VERSION);

        long calculatedSize = CassandraException.serializer.serializedSize(exception, VERSION);
        long actualSize = out.getLength();

        assertEquals("Serialized size calculation mismatch for " + exception.getClass().getSimpleName(),
                    calculatedSize, actualSize);
    }
}
