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
package org.apache.cassandra.distributed.test.streaming;

import java.io.IOException;

import com.google.common.base.Throwables;
import org.junit.Test;

import org.apache.cassandra.streaming.StreamSession;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoundExceptionTest
{
    private static final int LIMIT = 2;

    @Test
    public void testSingleException()
    {
        Throwable boundedStackTrace = StreamSession.boundStackTrace(new RuntimeException("test exception"), LIMIT);

        String expectedStackTrace = "java.lang.RuntimeException: test exception\n" +
                                        "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testSingleException(BoundExceptionTest.java:37)\n" +
                                        "\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n";

        assertEquals(expectedStackTrace,Throwables.getStackTraceAsString(boundedStackTrace));
        assertEquals(boundedStackTrace.getStackTrace().length, LIMIT);
    }

    @Test
    public void testNestedException()
    {
        Throwable exceptionToTest = new RuntimeException(new IllegalArgumentException("the disk /foo/var is bad", new IOException("Bad disk somewhere")));
        Throwable boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);

        String expectedStackTrace1 = "java.lang.RuntimeException: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                    "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testNestedException(BoundExceptionTest.java:50)\n";
        String expectedStackTrace2 = "java.lang.IllegalArgumentException: the disk /foo/var is bad\n";
        String expectedStackTrace3 = "java.io.IOException: Bad disk somewhere\n";

        String boundedStackTraceAsString = Throwables.getStackTraceAsString(boundedStackTrace);

        assertTrue(boundedStackTraceAsString.contains(expectedStackTrace1));
        assertTrue(boundedStackTraceAsString.contains(expectedStackTrace2));
        assertTrue(boundedStackTraceAsString.contains(expectedStackTrace3));

        while (boundedStackTrace != null)
        {
            assertEquals(boundedStackTrace.getStackTrace().length, LIMIT);
            boundedStackTrace = boundedStackTrace.getCause();
        }
    }

    @Test
    public void testExceptionCycle()
    {
        Exception e1 = new Exception("Test exception 1");
        Exception e2 = new RuntimeException("Test exception 2");

        e1.initCause(e2);
        e2.initCause(e1);

        Throwable boundedStackTrace = StreamSession.boundStackTrace(e1, LIMIT);
        String expectedStackTrace1 = "java.lang.Exception: Test exception 1\n" +
                                    "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testExceptionCycle(BoundExceptionTest.java:74)\n";
        String expectedStackTrace2 = "java.lang.RuntimeException: Test exception 2\n" +
                                     "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testExceptionCycle(BoundExceptionTest.java:75)\n";
        String expectedStackTrace3 = "[CIRCULAR REFERENCE: java.lang.Exception: Test exception 1]\n";

        String boundedStackTraceAsString = Throwables.getStackTraceAsString(boundedStackTrace);

        assertTrue(boundedStackTraceAsString.contains(expectedStackTrace1));
        assertTrue(boundedStackTraceAsString.contains(expectedStackTrace2));
        assertTrue(boundedStackTraceAsString.contains(expectedStackTrace3));
        assertEquals(boundedStackTrace.getStackTrace().length, LIMIT);
    }

    @Test
    public void testEmptyStackTrace()
    {
        Throwable exceptionToTest = new NullPointerException("there are words here");
        exceptionToTest.setStackTrace(new StackTraceElement[0]);

        Throwable boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);
        String expectedStackTrace = "java.lang.NullPointerException: there are words here\n";

        assertEquals(expectedStackTrace,Throwables.getStackTraceAsString(boundedStackTrace));
        assertEquals(boundedStackTrace.getStackTrace().length, 0);
    }

    @Test
    public void testLimitLargerThanStackTrace()
    {
        Throwable exceptionToTest = new NullPointerException("there are words here");
        Throwable boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);
        Throwable reboundedStackTrace = StreamSession.boundStackTrace(boundedStackTrace, LIMIT + 2);

        assertEquals(reboundedStackTrace, boundedStackTrace);
        assertEquals(reboundedStackTrace.getStackTrace().length, LIMIT);
    }

    @Test
    public void testEmptyNestedStackTrace()
    {
        Throwable exceptionToTest = new RuntimeException(new IllegalArgumentException("the disk /foo/var is bad", new IOException("Bad disk somewhere")));

        exceptionToTest.setStackTrace(new StackTraceElement[0]);
        exceptionToTest.getCause().setStackTrace(new StackTraceElement[0]);
        exceptionToTest.getCause().getCause().setStackTrace(new StackTraceElement[0]);

        Throwable boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);
        String expectedStackTrace = "java.lang.RuntimeException: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                     "Caused by: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                     "Caused by: java.io.IOException: Bad disk somewhere\n";

        String boundedStackTraceAsString = Throwables.getStackTraceAsString(boundedStackTrace);

        assertEquals(boundedStackTraceAsString, expectedStackTrace);

        while (boundedStackTrace != null)
        {
            assertEquals(boundedStackTrace.getStackTrace().length, 0);
            boundedStackTrace = boundedStackTrace.getCause();
        }
    }
}
