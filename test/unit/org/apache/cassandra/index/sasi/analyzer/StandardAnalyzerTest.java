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

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.junit.Test;

import org.apache.cassandra.serializers.UTF8Serializer;

import static org.junit.Assert.assertEquals;

public class StandardAnalyzerTest
{
    @Test
    public void testTokenizationAscii() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/apache_license_header.txt");

        StandardTokenizerOptions options = new StandardTokenizerOptions.OptionsBuilder()
                .maxTokenLength(5).build();
        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(options);

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(is);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(67, tokens.size());
    }

    @Test
    public void testTokenizationLoremIpsum() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/lorem_ipsum.txt");

        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(StandardTokenizerOptions.getDefaultOptions());

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(is);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(62, tokens.size());

    }

    @Test
    public void testTokenizationJaJp1() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/ja_jp_1.txt");

        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(StandardTokenizerOptions.getDefaultOptions());

        tokenizer.reset(is);
        List<ByteBuffer> tokens = new ArrayList<>();
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(210, tokens.size());
    }

    @Test
    public void testTokenizationJaJp2() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/ja_jp_2.txt");

        StandardTokenizerOptions options = new StandardTokenizerOptions.OptionsBuilder().stemTerms(true)
                .ignoreStopTerms(true).alwaysLowerCaseTerms(true).build();
        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(options);

        tokenizer.reset(is);
        List<ByteBuffer> tokens = new ArrayList<>();
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(57, tokens.size());
    }

    @Test
    public void testTokenizationRuRu1() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/ru_ru_1.txt");
        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(StandardTokenizerOptions.getDefaultOptions());

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(is);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(456, tokens.size());
    }

    @Test
    public void testTokenizationZnTw1() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/zn_tw_1.txt");
        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(StandardTokenizerOptions.getDefaultOptions());

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(is);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(963, tokens.size());
    }

    @Test
    public void testTokenizationAdventuresOfHuckFinn() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/adventures_of_huckleberry_finn_mark_twain.txt");

        StandardTokenizerOptions options = new StandardTokenizerOptions.OptionsBuilder().stemTerms(true)
                .ignoreStopTerms(true).useLocale(Locale.ENGLISH)
                .alwaysLowerCaseTerms(true).build();
        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(options);

        List<ByteBuffer> tokens = new ArrayList<>();
        tokenizer.reset(is);
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(37739, tokens.size());
    }

    @Test
    public void testSkipStopWordBeforeStemmingFrench() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
               .getResourceAsStream("tokenization/french_skip_stop_words_before_stemming.txt");

        StandardTokenizerOptions options = new StandardTokenizerOptions.OptionsBuilder().stemTerms(true)
                .ignoreStopTerms(true).useLocale(Locale.FRENCH)
                .alwaysLowerCaseTerms(true).build();
        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(options);

        List<ByteBuffer> tokens = new ArrayList<>();
        List<String> words = new ArrayList<>();
        tokenizer.reset(is);
        while (tokenizer.hasNext())
        {
            final ByteBuffer nextToken = tokenizer.next();
            tokens.add(nextToken);
            words.add(UTF8Serializer.instance.deserialize(nextToken.duplicate()));
        }

        assertEquals(4, tokens.size());
        assertEquals("dans", words.get(0));
        assertEquals("plui", words.get(1));
        assertEquals("chanson", words.get(2));
        assertEquals("connu", words.get(3));
    }

    @Test
    public void tokenizeDomainNamesAndUrls() throws Exception
    {
        InputStream is = StandardAnalyzerTest.class.getClassLoader()
                .getResourceAsStream("tokenization/top_visited_domains.txt");

        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(StandardTokenizerOptions.getDefaultOptions());
        tokenizer.reset(is);

        List<ByteBuffer> tokens = new ArrayList<>();
        while (tokenizer.hasNext())
            tokens.add(tokenizer.next());

        assertEquals(15, tokens.size());
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

        StandardAnalyzer tokenizer = new StandardAnalyzer();
        tokenizer.init(StandardTokenizerOptions.getDefaultOptions());

        List<ByteBuffer> tokens = new ArrayList<>();
        for (ByteBuffer bb : bbToTokenize)
        {
            tokenizer.reset(bb);
            while (tokenizer.hasNext())
                tokens.add(tokenizer.next());
        }
        assertEquals(10, tokens.size());
    }
}
