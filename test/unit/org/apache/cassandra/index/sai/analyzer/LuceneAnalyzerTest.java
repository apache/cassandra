
/*
 * All changes to the original code are Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Charsets;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.analysis.Analyzer;

import static org.junit.Assert.assertArrayEquals;

public class LuceneAnalyzerTest
{
    @Test
    public void testCzechStem() throws Exception
    {
        String json = "[\n" +
                      "{\"tokenizer\":\"standard\"},\n" +
                      "{\"filter\":\"lowercase\"},\n" +
                      "{\"filter\":\"czechstem\"}\n" +
                      "]\n";
        String testString = "pánové";
        String[] expected = new String[]{ "pán" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testPattern() throws Exception
    {
        String json = "[\n" +
                      "{\"tokenizer\":\"simplepattern\", \"pattern\":\"[0123456789]{3}\"}" +
                      "]";
        String testString = "fd-786-335-514-x";
        String[] expected = new String[]{ "786", "335", "514"  };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testNgram() throws Exception
    {
        String json = "[\n" +
                      "{\"tokenizer\":\"ngram\", \"minGramSize\":\"2\", \"maxGramSize\":\"3\"},\n" +
                      "{\"filter\":\"lowercase\"}\n" +
                      "]";
        String testString = "DoG";
        String[] expected = new String[]{ "do", "dog", "og" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testPorterStem1() throws Exception
    {
        String json = "[\n" +
                      "{\"tokenizer\":\"whitespace\"},\n" +
                      "{\"filter\":\"porterstem\"}\n" +
                      "]";
        String testString = "dogs withering in the windy";
        String[] expected = new String[]{ "dog", "wither", "in", "the", "windi" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testPorterStem2() throws Exception
    {
        String json = "[\n" +
                      "{\"tokenizer\":\"whitespace\"},\n" +
                      "{\"filter\":\"porterstem\"}\n" +
                      "]";
        String testString = "apples orcharding";
        String[] expected = new String[]{ "appl", "orchard"};
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    public static List<String> tokenize(String testString, String json) throws Exception
    {
        Analyzer luceneAnalyzer = JSONAnalyzerParser.parse(json);
        LuceneAnalyzer analyzer = new LuceneAnalyzer(UTF8Type.instance, luceneAnalyzer, new HashMap<String, String>());

        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes(Charsets.UTF_8));
        analyzer.reset(toAnalyze);
        ByteBuffer analyzed = null;

        List<String> list = new ArrayList();

        while (analyzer.hasNext())
        {
            analyzed = analyzer.next();
            list.add(ByteBufferUtil.string(analyzed, Charsets.UTF_8));
        }

        analyzer.end();

        return list;
    }
}