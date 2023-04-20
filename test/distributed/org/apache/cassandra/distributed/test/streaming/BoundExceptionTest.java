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

import org.junit.Test;

import org.apache.cassandra.streaming.StreamSession;

import static org.junit.Assert.assertEquals;

public class BoundExceptionTest
{
    private static final int LIMIT = 2;

    @Test
    public void testSingleException()
    {
        Throwable exceptionToTest = exception("test exception");
        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT, new StringBuilder());

        String expectedStackTrace = "java.lang.RuntimeException: test exception\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:1)\n";

        assertEquals(expectedStackTrace,boundedStackTrace.toString());
    }

    @Test
    public void testNestedException()
    {
        Throwable exceptionToTest = exception(exception("the disk /foo/var is bad", exception("Bad disk somewhere")));
        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT, new StringBuilder());

        String expectedStackTrace = "java.lang.RuntimeException: java.lang.RuntimeException: the disk /foo/var is bad\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:1)\n" +
                                    "java.lang.RuntimeException: the disk /foo/var is bad\n" +
                                    "java.lang.RuntimeException: Bad disk somewhere\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:1)\n";

        assertEquals(expectedStackTrace, boundedStackTrace.toString());
    }

    @Test
    public void testExceptionCycle()
    {
        Exception e1 = exception("Test exception 1");
        Exception e2 = exception("Test exception 2");

        e1.initCause(e2);
        e2.initCause(e1);

        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(e1, LIMIT, new StringBuilder());
        String expectedStackTrace = "java.lang.RuntimeException: Test exception 1\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:1)\n" +
                                    "java.lang.RuntimeException: Test exception 2\n" +
                                    "[CIRCULAR REFERENCE: java.lang.RuntimeException: Test exception 1]\n";

        assertEquals(expectedStackTrace, boundedStackTrace.toString());
    }

    @Test
    public void testEmptyStackTrace()
    {
        Throwable exceptionToTest = exception("there are words here", 0);

        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT, new StringBuilder());
        String expectedStackTrace = "java.lang.RuntimeException: there are words here\n";

        assertEquals(expectedStackTrace,boundedStackTrace.toString());
    }

    @Test
    public void testEmptyNestedStackTrace()
    {
        Throwable exceptionToTest = exception(exception("the disk /foo/var is bad", exception("Bad disk somewhere"), 0), 0);

        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT, new StringBuilder());
        String expectedStackTrace = "java.lang.RuntimeException: java.lang.RuntimeException: the disk /foo/var is bad\n" +
                                    "java.lang.RuntimeException: the disk /foo/var is bad\n" +
                                    "java.lang.RuntimeException: Bad disk somewhere\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:1)\n";

        assertEquals(expectedStackTrace, boundedStackTrace.toString());
    }

    @Test
    public void testLimitLargerThanStackTrace()
    {
        Throwable exceptionToTest = exception(exception("the disk /foo/var is bad", exception("Bad disk somewhere")), 1);

        StringBuilder boundedStackTrace = StreamSession.boundStackTrace(exceptionToTest, LIMIT, new StringBuilder());
        String expectedStackTrace = "java.lang.RuntimeException: java.lang.RuntimeException: the disk /foo/var is bad\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "java.lang.RuntimeException: the disk /foo/var is bad\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "java.lang.RuntimeException: Bad disk somewhere\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:0)\n" +
                                    "\torg.apache.cassandra.distributed.test.streaming.BoundExceptionTest.method(BoundExceptionTest.java:1)\n";

        assertEquals(expectedStackTrace, boundedStackTrace.toString());
    }

    private static StackTraceElement[] frames(int length)
    {
        StackTraceElement[] frames = new StackTraceElement[length];
        for (int i = 0; i < length; i++)
            frames[i] = new StackTraceElement(BoundExceptionTest.class.getCanonicalName(), "method", BoundExceptionTest.class.getSimpleName() + ".java", i);
        return frames;
    }

    private static RuntimeException exception(String msg)
    {
        return exception(msg, null);
    }

    private static RuntimeException exception(String msg, int length)
    {
        return exception(msg, null, length);
    }

    private static RuntimeException exception(Throwable cause)
    {
        return exception(null, cause);
    }

    private static RuntimeException exception(Throwable cause, int length)
    {
        return exception(null, cause, length);
    }

    private static RuntimeException exception(String msg, Throwable cause)
    {
        return exception(msg, cause, LIMIT * 2);
    }

    private static RuntimeException exception(String msg, Throwable cause, int length)
    {
        RuntimeException e;
        if (msg != null && cause != null) e = new RuntimeException(msg, cause);
        else if (msg != null) e = new RuntimeException(msg);
        else if (cause != null) e = new RuntimeException(cause);
        else e = new RuntimeException();
        e.setStackTrace(frames(length));
        return e;
    }
}
