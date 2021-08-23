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
package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.io.IOUtils;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DelimiterAnalyzerTest
{

    @Test
    public void caseSensitiveAnalizer() throws Exception
    {
        DelimiterAnalyzer analyzer = new DelimiterAnalyzer();

        analyzer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, " ");
                }},
            UTF8Type.instance);

        String testString = "Nip it in the bud";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        StringBuilder output = new StringBuilder();
        while (analyzer.hasNext())
            output.append(ByteBufferUtil.string(analyzer.next()) + (analyzer.hasNext() ? ' ' : ""));

        Assert.assertEquals(testString, output.toString());
        Assert.assertFalse(testString.toLowerCase().equals(output.toString()));
    }

    @Test
    public void testBlankEntries() throws Exception
    {
        DelimiterAnalyzer analyzer = new DelimiterAnalyzer();

        analyzer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, ",");
                }},
            UTF8Type.instance);

        String testString = ",Nip,,,,it,,,in,,the,bud,,,";
        ByteBuffer toAnalyze = ByteBuffer.wrap(testString.getBytes());
        analyzer.reset(toAnalyze);
        StringBuilder output = new StringBuilder();
        while (analyzer.hasNext())
            output.append(ByteBufferUtil.string(analyzer.next()) + (analyzer.hasNext() ? ',' : ""));

        Assert.assertEquals("Nip,it,in,the,bud", output.toString());
        Assert.assertFalse(testString.toLowerCase().equals(output.toString()));
    }

    @Test(expected = ConfigurationException.class)
    public void ensureIncompatibleInputOnCollectionTypeSkipped()
    {
        new DelimiterAnalyzer().validate(Collections.emptyMap(),
                                         ColumnMetadata.regularColumn("a", "b", "c", SetType.getInstance(UTF8Type.instance, true)));
    }

    @Test(expected = ConfigurationException.class)
    public void ensureIncompatibleInputSkipped()
    {
        new DelimiterAnalyzer().validate(Collections.emptyMap(),
                                         ColumnMetadata.regularColumn("a", "b", "c", Int32Type.instance));
    }

    @Test
    public void testTokenizationLoremIpsum() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/lorem_ipsum.txt")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, " ");
                }},
            UTF8Type.instance);

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(bb);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(69, tokens.size());

    }

    @Test
    public void testTokenizationJaJp1() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/ja_jp_1.txt")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, "。");
                }},
            UTF8Type.instance);

        tokenizer.reset(bb);
        List<ByteBuffer> tokens = new ArrayList<>();
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(4, tokens.size());
    }

    @Test
    public void testTokenizationJaJp2() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/ja_jp_2.txt")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, "。");
                }},
            UTF8Type.instance);

        tokenizer.reset(bb);
        List<ByteBuffer> tokens = new ArrayList<>();
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(2, tokens.size());
    }

    @Test
    public void testTokenizationRuRu1() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/ru_ru_1.txt")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, " ");
                }},
            UTF8Type.instance);

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(bb);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(447, tokens.size());
    }

    @Test
    public void testTokenizationZnTw1() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/zn_tw_1.txt")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, " ");
                }},
            UTF8Type.instance);

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(bb);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(403, tokens.size());
    }

    @Test
    public void testTokenizationAdventuresOfHuckFinn() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/adventures_of_huckleberry_finn_mark_twain.txt")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, " ");
                }},
            UTF8Type.instance);

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(bb);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(104594, tokens.size());
    }

    @Test
    public void testWorldCities() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/world_cities_a.csv")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, ",");
                }},
            UTF8Type.instance);

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(bb);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(122265, tokens.size());
    }

    @Test
    public void tokenizeDomainNamesAndUrls() throws Exception
    {
        ByteBuffer bb = ByteBuffer.wrap(IOUtils.toByteArray(
                DelimiterAnalyzerTest.class.getClassLoader().getResourceAsStream("tokenization/top_visited_domains.txt")));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, " ");
                }},
            UTF8Type.instance);

        tokenizer.reset(bb);

        List<ByteBuffer> tokens = new ArrayList<>();
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(12, tokens.size());
    }

    @Test
    public void testReuseAndResetTokenizerInstance() throws Exception
    {
        List<ByteBuffer> bbToTokenize = new ArrayList<>();
        bbToTokenize.add(ByteBuffer.wrap("Nip it in the bud".getBytes()));
        bbToTokenize.add(ByteBuffer.wrap("I couldn’t care less".getBytes()));
        bbToTokenize.add(ByteBuffer.wrap("One and the same".getBytes()));
        bbToTokenize.add(ByteBuffer.wrap("The squeaky wheel gets the grease.".getBytes()));
        bbToTokenize.add(ByteBuffer.wrap("The pen is mightier than the sword.".getBytes()));

        DelimiterAnalyzer tokenizer = new DelimiterAnalyzer();

        tokenizer.init(
            new HashMap()
                {{
                    put(DelimiterTokenizingOptions.DELIMITER, " ");
                }},
            UTF8Type.instance);

        List<ByteBuffer> tokens = new ArrayList<>();
        for (ByteBuffer bb : bbToTokenize)
        {
            tokenizer.reset(bb);
            while (tokenizer.hasNext())
                tokens.add(tokenizer.next());
        }
        assertEquals(26, tokens.size());
    }

}
