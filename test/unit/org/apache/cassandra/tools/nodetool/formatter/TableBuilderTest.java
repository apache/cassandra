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

package org.apache.cassandra.tools.nodetool.formatter;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableBuilderTest
{
    @Test
    public void testEmptyRow()
    {
        TableBuilder table = new TableBuilder();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos))
        {
            table.printTo(out);
        }
        assertEquals("", baos.toString());
    }

    @Test
    public void testOneRow()
    {
        TableBuilder table = new TableBuilder();

        table.add("a", "bb", "ccc");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos))
        {
            table.printTo(out);
        }
        assertEquals(String.format("a bb ccc%n"), baos.toString());
    }

    @Test
    public void testRows()
    {
        TableBuilder table = new TableBuilder();
        table.add("a", "bb", "ccc");
        table.add("aaa", "bb", "c");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos))
        {
            table.printTo(out);
        }
        assertEquals(String.format("a   bb ccc%naaa bb c  %n"), baos.toString());
    }

    @Test
    public void testNullColumn()
    {
        TableBuilder table = new TableBuilder();
        table.add("a", "b", "c");
        table.add("a", null, "c");
        table.add("a", null, null);
        table.add(null, "b", "c");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos))
        {
            table.printTo(out);
        }
        assertEquals(String.format("a b c%na   c%na    %n  b c%n"), baos.toString());
    }

    @Test
    public void testRowsOfDifferentSize()
    {
        TableBuilder table = new TableBuilder();
        table.add("a", "b", "c");
        table.add("a", "b", "c", "d", "e");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos))
        {
            table.printTo(out);
        }
        assertEquals(baos.toString(), String.format("a b c    %na b c d e%n"), baos.toString());
    }

    @Test
    public void testDelimiter()
    {
        TableBuilder table = new TableBuilder('\t');

        table.add("a", "bb", "ccc");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream out = new PrintStream(baos))
        {
            table.printTo(out);
        }
        assertEquals(String.format("a\tbb\tccc%n"), baos.toString());
    }
}