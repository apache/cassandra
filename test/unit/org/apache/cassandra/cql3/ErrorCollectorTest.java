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
package org.apache.cassandra.cql3;

import org.antlr.runtime.CharStream;
import org.antlr.runtime.Token;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ErrorCollectorTest
{
    @Test
    public void testAppendSnippetWithEmptyQuery()
    {
        String query = ";";

        ErrorCollector collector = new ErrorCollector(query);

        StringBuilder builder = new StringBuilder();

        Token from = new MockToken(1, 0, ";");
        Token to = new MockToken(1, 0, ";");
        Token offending = new MockToken(1, 0, ";");

        collector.appendSnippet(builder, from, to, offending);

        String expected = " ([;])";

        assertEquals(expected, builder.toString());
    }

    @Test
    public void testAppendSnippetWithOneLines()
    {
        String query = "select * from users where user_name = ''test'';";

        ErrorCollector collector = new ErrorCollector(query);

        StringBuilder builder = new StringBuilder();

        Token from = new MockToken(1, 25, " ");
        Token to = new MockToken(1, 46, ";");
        Token offending = new MockToken(1, 40, "test");

        collector.appendSnippet(builder, from, to, offending);

        String expected = " (... user_name = ''[test]'';)";

        assertEquals(expected, builder.toString());
    }

    @Test
    public void testAppendSnippetOnSecondLine()
    {
        String query = "select * from users\n" +
                "where user_name = ''test'';";

        ErrorCollector collector = new ErrorCollector(query);

        StringBuilder builder = new StringBuilder();

        Token from = new MockToken(2, 5, " ");
        Token to = new MockToken(2, 26, ";");
        Token offending = new MockToken(2, 20, "test");

        collector.appendSnippet(builder, from, to, offending);

        String expected = " (... user_name = ''[test]'';)";

        assertEquals(expected, builder.toString());
    }

    @Test
    public void testAppendSnippetWithSnippetOverTwoLines()
    {
        String query = "select * from users where user_name \n" +
                "= ''test'';";

        ErrorCollector collector = new ErrorCollector(query);

        StringBuilder builder = new StringBuilder();

        Token from = new MockToken(1, 20, "where");
        Token to = new MockToken(2, 9, "'");
        Token offending = new MockToken(2, 4, "test");

        collector.appendSnippet(builder, from, to, offending);

        String expected = " (...where user_name = ''[test]''...)";

        assertEquals(expected, builder.toString());
    }

    @Test
    public void testAppendSnippetWithInvalidToToken()
    {
        String query = "CREATE TABLE test (a int PRIMARY KEY, b set<int>;";

        ErrorCollector collector = new ErrorCollector(query);

        StringBuilder builder = new StringBuilder();

        Token from = new MockToken(1, 32, " ");
        Token to = new MockToken(0, -1, "<no text>");
        Token offending = new MockToken(1, 48, ";");

        collector.appendSnippet(builder, from, to, offending);
        assertEquals("", builder.toString());
    }

    private final static class MockToken implements Token
    {
        /**
         * The line number on which this token was matched; line=1..n
         */
        private int line;

        /**
         * The index of the first character relative to the beginning of the line 0..n-1
         */
        private int charPositionInLine;

        /**
         * The text of the token
         */
        private String text;

        public MockToken(int line, int charPositionInLine, String text)
        {
            this.line = line;
            this.charPositionInLine = charPositionInLine;
            this.text = text;
        }

        @Override
        public int getChannel()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getCharPositionInLine()
        {
            return charPositionInLine;
        }

        @Override
        public CharStream getInputStream()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLine()
        {
            return line;
        }

        @Override
        public String getText()
        {
            return text;
        }

        @Override
        public int getTokenIndex()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getType()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChannel(int channel)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setCharPositionInLine(int charPositionInLine)
        {
            this.charPositionInLine = charPositionInLine;
        }

        @Override
        public void setInputStream(CharStream inputStream)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setLine(int line)
        {
            this.line = line;
        }

        @Override
        public void setText(String text)
        {
            this.text = text;
        }

        @Override
        public void setTokenIndex(int tokenIndex)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setType(int type)
        {
            throw new UnsupportedOperationException();
        }
    }
}
