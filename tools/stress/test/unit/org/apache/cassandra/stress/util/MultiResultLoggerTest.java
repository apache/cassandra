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

package org.apache.cassandra.stress.util;


import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import static org.junit.Assert.*;

public class MultiResultLoggerTest
{

    public static final OutputStream NOOP = new OutputStream()
    {
        public void write(int b) throws IOException
        {
        }
    };

    @Test
    public void delegatesToInitialPrintStream() throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(output, true);
        MultiResultLogger underTest = new MultiResultLogger(printStream);

        underTest.println("Very important result");

        assertEquals("Very important result\n", output.toString());
    }

    @Test
    public void printingExceptions() throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(output, true);
        MultiResultLogger underTest = new MultiResultLogger(printStream);

        underTest.printException(new RuntimeException("Bad things"));

        String stackTrace = output.toString();
        assertTrue("Expected strack trace to be printed but got: " + stackTrace, stackTrace.startsWith("java.lang.RuntimeException: Bad things\n" +
                                                "\tat org.apache.cassandra.stress.util.MultiResultLoggerTest.printingExceptions"));
    }

    @Test
    public void delegatesToAdditionalPrintStreams() throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream additionalPrintStream = new PrintStream(output, true);
        MultiResultLogger underTest = new MultiResultLogger(new PrintStream(NOOP));

        underTest.addStream(additionalPrintStream);
        underTest.println("Very important result");

        assertEquals("Very important result\n", output.toString());
    }

    @Test
    public void delegatesPrintfToAdditionalPrintStreams() throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream additionalPrintStream = new PrintStream(output, true);
        MultiResultLogger underTest = new MultiResultLogger(new PrintStream(NOOP));

        underTest.addStream(additionalPrintStream);
        underTest.printf("%s %s %s", "one", "two", "three");

        assertEquals("one two three", output.toString());
    }

    @Test
    public void delegatesPrintlnToAdditionalPrintStreams() throws Exception
    {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream additionalPrintStream = new PrintStream(output, true);
        MultiResultLogger underTest = new MultiResultLogger(new PrintStream(NOOP));

        underTest.addStream(additionalPrintStream);
        underTest.println();

        assertEquals("\n", output.toString());
    }
}