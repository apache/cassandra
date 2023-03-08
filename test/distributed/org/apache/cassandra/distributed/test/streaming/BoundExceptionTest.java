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
import java.util.HashSet;

import com.google.common.base.Throwables;
import org.junit.Test;

import org.apache.cassandra.streaming.StreamSession;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BoundExceptionTest
{
    private static final int LIMIT = 2;

    @Test
    public void testSingleException() {
        try {
            throw new RuntimeException("test exception");
        } catch (Exception e) {
            StreamSession.boundStackTrace(e, LIMIT, new HashSet<>());
            String expectedStackTrace = "java.lang.RuntimeException: test exception\n" +
                                        "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testSingleException(BoundExceptionTest.java:38)\n" +
                                        "\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n";
            assertEquals(expectedStackTrace,Throwables.getStackTraceAsString(e));
            assertEquals(e.getStackTrace().length, LIMIT);
        }
    }

    @Test
    public void testNestedException() {
        try {
            throw new RuntimeException(new IllegalArgumentException("the disk /foo/var is bad", new IOException("Bad disk somewhere")));
        } catch (Exception e) {
            StreamSession.boundStackTrace(e, LIMIT, new HashSet<>());
            String expectedStackTrace = "java.lang.RuntimeException: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                        "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testNestedException(BoundExceptionTest.java:52)\n" +
                                        "\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n" +
                                        "Caused by: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                        "\t... " + LIMIT + " more\n" +
                                        "Caused by: java.io.IOException: Bad disk somewhere\n" +
                                        "\t... " + LIMIT + " more\n";
            assertEquals(expectedStackTrace,Throwables.getStackTraceAsString(e));
            assertEquals(e.getStackTrace().length, LIMIT);
        }
    }

    @Test
    public void testExceptionCycle()
    {
        try {
            throw new Exception("Test exception 1");
        }
        catch (Exception e1) {
            try {
                throw new RuntimeException("Test exception 2");
            }
            catch (Exception e2) {
                e1.initCause(e2);
                e2.initCause(e1);

                causeCycle(e1);
            }
        }
    }

    private static void causeCycle(Exception e) {
        try {
            throw e;
        } catch (Exception e1) {
            try {
                StreamSession.boundStackTrace(e1, LIMIT, new HashSet<>());
            }
            catch (Exception e2) {
                String expectedStackTrace = "java.lang.IllegalArgumentException: Exception cycle detected";
                assertTrue(Throwables.getStackTraceAsString(e2).contains(expectedStackTrace));
            }
        }
    }

    @Test
    public void testEmptyStackTrace() {
        try {
            throw new NullPointerException("there are words here");
        } catch (Exception e) {
            e.setStackTrace(new StackTraceElement[0]);
            StreamSession.boundStackTrace(e, LIMIT, new HashSet<>());
            String expectedStackTrace = "java.lang.NullPointerException: there are words here\n";
            assertEquals(expectedStackTrace,Throwables.getStackTraceAsString(e));
            assertEquals(e.getStackTrace().length, 0);
        }
    }

    @Test
    public void testNullException() {
        try {
            throw null;
        } catch (Exception e) {
            StreamSession.boundStackTrace(e, LIMIT, new HashSet<>());
            String expectedException = "java.lang.NullPointerException\n" +
                                       "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testNullException(BoundExceptionTest.java:116)\n" +
                                       "\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n";
            assertEquals(expectedException, Throwables.getStackTraceAsString(e));
        }
    }

    @Test
    public void testEmptyNestedStackTrace() {
        try {
            throw new RuntimeException(new IllegalArgumentException("the disk /foo/var is bad", new IOException("Bad disk somewhere")));
        } catch (Exception e) {
            e.setStackTrace(new StackTraceElement[0]);
            e.getCause().setStackTrace(new StackTraceElement[0]);
            StreamSession.boundStackTrace(e, LIMIT, new HashSet<>());
            String expectedException = "java.lang.RuntimeException: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                       "Caused by: java.lang.IllegalArgumentException: the disk /foo/var is bad\n" +
                                       "Caused by: java.io.IOException: Bad disk somewhere\n" +
                                       "\tat org.apache.cassandra.distributed.test.streaming.BoundExceptionTest.testEmptyNestedStackTrace(BoundExceptionTest.java:129)\n" +
                                       "\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n";
            assertEquals(expectedException, Throwables.getStackTraceAsString(e));
        }
    }


}
