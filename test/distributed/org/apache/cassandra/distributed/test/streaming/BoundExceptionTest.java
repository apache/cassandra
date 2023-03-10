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

import org.junit.Test;

import org.apache.cassandra.streaming.StreamSession;

import static org.junit.Assert.assertEquals;

public class BoundExceptionTest
{
    private static final int LIMIT = 2;

    @Test
    public void testSingleException()
    {
        Throwable exceptionToTest = new RuntimeException("test exception");
        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);

        String expectedStackTrace = "java.lang.RuntimeException: test exception\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testSingleException(BoundExceptionTest.java:35)\n" +
                                    "\tjava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)";

        assertEquals(expectedStackTrace,boundedStackTrace.toString());
        assertEquals(boundedStackTrace.toString().split(System.lineSeparator()).length - 1, LIMIT);
    }

    @Test
    public void testNestedException()
    {
        Throwable exceptionToTest = new RuntimeException(new IllegalArgumentException("the disk /foo/var is bad", new IOException("Bad disk somewhere")));
        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);

        String expectedStackTrace1 = "java.lang.RuntimeException: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                     "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testNestedException(BoundExceptionTest.java:49)\n" +
                                     "\tjava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
                                     "java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                     "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testNestedException(BoundExceptionTest.java:49)\n" +
                                     "\tjava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
                                     "java.io.IOException: Bad disk somewhere\n" +
                                     "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testNestedException(BoundExceptionTest.java:49)\n" +
                                     "\tjava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)";

        String boundedStackTraceAsString = boundedStackTrace.toString();

        assertEquals(boundedStackTraceAsString, expectedStackTrace1);
    }

    @Test
    public void testExceptionCycle()
    {
        Exception e1 = new Exception("Test exception 1");
        Exception e2 = new RuntimeException("Test exception 2");

        e1.initCause(e2);
        e2.initCause(e1);

        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(e1, LIMIT);
        String expectedStackTrace = "java.lang.Exception: Test exception 1\n" +
                                     "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testExceptionCycle(BoundExceptionTest.java:70)\n" +
                                     "\tjava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
                                     "java.lang.RuntimeException: Test exception 2\n" +
                                     "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testExceptionCycle(BoundExceptionTest.java:71)\n" +
                                     "\tjava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
                                     "[CIRCULAR REFERENCE: java.lang.Exception: Test exception 1]";

        assertEquals(expectedStackTrace, boundedStackTrace.toString());
    }

    @Test
    public void testEmptyStackTrace()
    {
        Throwable exceptionToTest = new NullPointerException("there are words here");
        exceptionToTest.setStackTrace(new StackTraceElement[0]);

        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);
        String expectedStackTrace = "java.lang.NullPointerException: there are words here\n";

        assertEquals(expectedStackTrace,boundedStackTrace.toString());
    }

    @Test
    public void testEmptyNestedStackTrace()
    {
        Throwable exceptionToTest = new RuntimeException(new IllegalArgumentException("the disk /foo/var is bad", new IOException("Bad disk somewhere")));
        exceptionToTest.setStackTrace(new StackTraceElement[0]);
        exceptionToTest.getCause().getCause().setStackTrace(new StackTraceElement[0]);

        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT);
        String expectedStackTrace = "java.lang.RuntimeException: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                    "java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testEmptyNestedStackTrace(BoundExceptionTest.java:103)\n" +
                                    "\tjava.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
                                    "java.io.IOException: Bad disk somewhere\n";

        assertEquals(expectedStackTrace, boundedStackTrace.toString());
    }
}
