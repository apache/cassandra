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

package org.apache.cassandra.index.sai.analyzer;

import java.nio.ByteBuffer;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests for the non-tokenizing analyzer
 */
public class NonTokenizingAnalyzerTest
{
    @Test
    public void asciiAnalyzer() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        options.setCaseSensitive(false);
        options.setAscii(true);
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Éppinger";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;

        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }

        String good = "eppinger";

        String result = ByteBufferUtil.string(analyzed);

        assertEquals(good, result);
    }

    @Test
    public void asciiAnalyzerFalse() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        options.setCaseSensitive(true);
        options.setAscii(false);
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Éppinger";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;

        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }

        String good = "Éppinger";

        String result = ByteBufferUtil.string(analyzed);

        assertEquals(good, result);
    }

    @Test
    public void caseInsensitiveAnalyzer() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        options.setCaseSensitive(false);
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        
        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }
        
        assertEquals(testString.toLowerCase(), ByteBufferUtil.string(analyzed));
    }

    @Test
    public void caseSensitiveAnalyzer() throws Exception
    {
        NonTokenizingOptions options = NonTokenizingOptions.getDefaultOptions();
        NonTokenizingAnalyzer analyzer = new NonTokenizingAnalyzer(UTF8Type.instance, options);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;
        
        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
        }
        
        assertNotEquals(testString.toLowerCase(), ByteBufferUtil.string(analyzed));
    }
}
