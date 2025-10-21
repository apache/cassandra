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

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;


public class RequestFailureReasonTest
{
    private static final RequestFailureReason[] REASONS = RequestFailureReason.values();
    private static final Object[][] EXPECTED_VALUES =
    {
    { 0, "UNKNOWN" },
    { 1, "READ_TOO_MANY_TOMBSTONES" },
    { 2, "TIMEOUT" },
    { 3, "INCOMPATIBLE_SCHEMA" },
    { 4, "READ_SIZE" },
    { 5, "NODE_DOWN" },
    { 6, "INDEX_NOT_AVAILABLE" },
    { 7, "READ_TOO_MANY_INDEXES" },
    { 8, "NOT_CMS" },
    { 9, "INVALID_ROUTING" },
    { 10, "COORDINATOR_BEHIND" },
    { 11, "RETRY_ON_DIFFERENT_TRANSACTION_SYSTEM" },
    { 503, "INDEX_BUILD_IN_PROGRESS" },
    };

    @Test
    public void testEnumCodesAndNames()
    {
        for (int i = 0; i < REASONS.length; i++)
        {
            assertEquals("RequestFailureReason code mismatch for " +
                         REASONS[i].name(), EXPECTED_VALUES[i][0], REASONS[i].code);
            assertEquals("RequestFailureReason name mismatch for code " +
                         REASONS[i].code, EXPECTED_VALUES[i][1], REASONS[i].name());
        }

        assertEquals("Number of RequestFailureReason enum constants has changed. Update the test.",
                     EXPECTED_VALUES.length, REASONS.length);
    }

    @Test
    public void testFromCode()
    {
        // Test valid codes
        for (Object[] expected : EXPECTED_VALUES)
        {
            int code = (Integer) expected[0];
            String name = (String) expected[1];
            assertEquals(RequestFailureReason.valueOf(name), RequestFailureReason.fromCode(code));
        }

        // Test invalid codes
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(200));
        assertEquals(RequestFailureReason.UNKNOWN, RequestFailureReason.fromCode(999));
        assertThatThrownBy(() -> RequestFailureReason.fromCode(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExceptionSubclassMapping()
    {
        // Create a subclass of UnknownTableException
        class CustomUnknownTableException extends IncompatibleSchemaException
        {
            public CustomUnknownTableException(String ks)
            {
                super(ks);
            }
        }
        
        // Verify the parent class still maps correctly
        assertEquals(RequestFailureReason.INCOMPATIBLE_SCHEMA,
                    RequestFailureReason.forException(new CustomUnknownTableException("ks")));
        
        // Test unmapped exception returns UNKNOWN
        assertEquals(RequestFailureReason.UNKNOWN, 
                    RequestFailureReason.forException(new RuntimeException("test")));
    }
}
