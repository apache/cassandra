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

import org.junit.Test;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.BaseRecognizer;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;

import static org.junit.Assert.*;

public class CqlParserTest
{
    @Test
    public void testAddErrorListener() throws Exception
    {
        SyntaxErrorCounter firstCounter = new SyntaxErrorCounter();
        SyntaxErrorCounter secondCounter = new SyntaxErrorCounter();

        CharStream stream = new ANTLRStringStream("SELECT * FORM users");
        CqlLexer lexer = new CqlLexer(stream);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.addErrorListener(firstCounter);
        parser.addErrorListener(secondCounter);

        parser.query();

        // ANTLR 3.5 reports 2 errors in the sentence above (missing FROM and missing EOF).
        assertTrue(firstCounter.count > 0);
        assertTrue(secondCounter.count > 0);
    }

    @Test
    public void testRemoveErrorListener() throws Exception
    {
        SyntaxErrorCounter firstCounter = new SyntaxErrorCounter();
        SyntaxErrorCounter secondCounter = new SyntaxErrorCounter();

        CharStream stream = new ANTLRStringStream("SELECT * FORM test;");
        CqlLexer lexer = new CqlLexer(stream);

        TokenStream tokenStream = new CommonTokenStream(lexer);
        CqlParser parser = new CqlParser(tokenStream);
        parser.addErrorListener(firstCounter);
        parser.addErrorListener(secondCounter);
        parser.removeErrorListener(secondCounter);

        parser.query();

        assertTrue(firstCounter.count > 0);
        assertEquals(0, secondCounter.count);
    }

    private static final class SyntaxErrorCounter implements ErrorListener
    {
        private int count;

        @Override
        public void syntaxError(BaseRecognizer recognizer, String[] tokenNames, RecognitionException e)
        {
            count++;
        }

        @Override
        public void syntaxError(BaseRecognizer recognizer, String errorMsg)
        {
            count++;
        }
    }
}
