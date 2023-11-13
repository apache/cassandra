
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
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;

import static org.junit.Assert.assertArrayEquals;

public class LuceneAnalyzerTest
{
    @Test
    public void testCzechStem() throws Exception
    {
        String json = "{\n" +
                      "\"tokenizer\":{\"name\":\"standard\"},\n" +
                      "\"filters\":[{\"name\":\"czechstem\"}]\n" +
                      "}\n";
        String testString = "pánové";
        String[] expected = new String[]{ "pán" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testPattern() throws Exception
    {
        String json = "{\n" +
                      "\"tokenizer\":{\"name\":\"simplepattern\", \"args\":{\"pattern\":\"[0123456789]{3}\"}}}";
        String testString = "fd-786-335-514-x";
        String[] expected = new String[]{ "786", "335", "514"  };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testNgram() throws Exception
    {
        String json = "{\n" +
                      "\"tokenizer\":{\"name\":\"ngram\", \"args\":{\"minGramSize\":\"2\", \"maxGramSize\":\"3\"}},\n" +
                      "\"filters\":[{\"name\":\"lowercase\"}]\n" +
                      "}";
        String testString = "DoG";
        String[] expected = new String[]{ "do", "dog", "og" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    // We override the defaults for ngram. This test ensures that our defaults are used and not lucene's
    @Test
    public void testNgramTokenizerDefaults() throws Exception
    {
        String json = "{\n" +
                      "\"tokenizer\":{\"name\":\"ngram\"},\n" +
                      "\"filters\":[{\"name\":\"lowercase\"}]\n" +
                      "}";
        String testString = "DoGs";
        // Default minGramSize is 3
        String[] expected = new String[]{ "dog", "dogs", "ogs" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testPorterStem1() throws Exception
    {
        String json = "{\n" +
                      "\"tokenizer\":{\"name\":\"whitespace\"},\n" +
                      "\"filters\":[{\"name\":\"porterstem\"}]\n" +
                      "}";
        String testString = "dogs withering in the windy";
        String[] expected = new String[]{ "dog", "wither", "in", "the", "windi" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testPorterStem2() throws Exception
    {
        String json = "{\n" +
                      "\"tokenizer\":{\"name\":\"whitespace\"},\n" +
                      "\"filters\":[{\"name\":\"porterstem\"}]\n" +
                      "}";
        String testString = "apples orcharding";
        String[] expected = new String[]{ "appl", "orchard"};
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testStopwordWithEscapedComma() throws Exception
    {
        // Need 4 backslashes to get through all of the parsing... this is an unlikely scenario, so it seems
        // acceptable. Note that in CQL, it'll just need 2 backslashes.
        String json = "{\"tokenizer\":{\"name\" : \"whitespace\"}," +
                      "\"filters\":[{\"name\":\"stop\", \"args\": " +
                      "{\"words\": \"one\\\\,stopword, test\"}}]}";
        // Put result in the middle to make sure that one,stopword and test are broken up and applied individually
        String testString = "one,stopword result test";
        String[] expected = new String[]{ "result" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test
    public void testStopwordWithSpace() throws Exception
    {
        // Need 4 backslashes to get through all of the parsing... this is an unlikely scenario, so it seems
        // acceptable. Note that in CQL, it'll just need 2 backslashes.
        String json = "{\"tokenizer\":{\"name\" : \"keyword\"}," +
                      "\"filters\":[{\"name\":\"stop\", \"args\": " +
                      "{\"words\": \"one stopword, test\"}}]}";
        // 'one stopword' is a single stopword, so it gets filtered out (note that the tokenizer is keyword, so
        // it doesn't get broken up into multiple tokens)
        String testString = "one stopword";
        List<String> list = tokenize(testString, json);
        assertArrayEquals(new String[]{}, list.toArray(new String[0]));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingSynonymArg() throws Exception
    {
        // The synonym filter takes a 'synonyms' argument, not a 'words' argument
        String json = "{\"tokenizer\":{\"name\" : \"keyword\"}," +
                      "\"filters\":[{\"name\":\"synonym\", \"args\": " +
                      "{\"words\": \"as => like\"}}]}";
        tokenize("irrelevant test string", json);
    }

    @Test
    public void testSynonmyMapping() throws Exception
    {
        // Need 4 backslashes to get through all of the parsing... this is an unlikely scenario, so it seems
        // acceptable. Note that in CQL, it'll just need 2 backslashes.
        String json = "{\"tokenizer\":{\"name\" : \"keyword\"}," +
                      "\"filters\":[{\"name\":\"synonym\", \"args\": " +
                      "{\"synonyms\": \"as => like\", \"analyzer\":\"" + WhitespaceAnalyzer.class.getName() + "\"}}]}";
        String testString = "as";
        String[] expected = new String[]{ "like" };
        List<String> list = tokenize(testString, json);
        assertArrayEquals(expected, list.toArray(new String[0]));
    }

    @Test(expected = RuntimeException.class)
    public void testMissingClassException() throws Exception
    {
        // Need 4 backslashes to get through all of the parsing... this is an unlikely scenario, so it seems
        // acceptable. Note that in CQL, it'll just need 2 backslashes.
        String json = "{\"tokenizer\":{\"name\" : \"keyword\"}," +
                      "\"filters\":[{\"name\":\"synonym\", \"args\": " +
                      "{\"synonyms\": \"as => like\", \"analyzer\":\"not-a-class\"}}]}";
        tokenize("irrelevant text", json);
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