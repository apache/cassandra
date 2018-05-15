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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.index.sasi.analyzer.filter.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.annotations.VisibleForTesting;

import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntObjectOpenHashMap;

public class StandardAnalyzer extends AbstractAnalyzer
{

    private static final Set<AbstractType<?>> VALID_ANALYZABLE_TYPES = new HashSet<AbstractType<?>>()
    {
        {
            add(UTF8Type.instance);
            add(AsciiType.instance);
        }
    };

    public enum TokenType
    {
        EOF(-1),
        ALPHANUM(0),
        NUM(6),
        SOUTHEAST_ASIAN(9),
        IDEOGRAPHIC(10),
        HIRAGANA(11),
        KATAKANA(12),
        HANGUL(13);

        private static final IntObjectMap<TokenType> TOKENS = new IntObjectOpenHashMap<>();

        static
        {
            for (TokenType type : TokenType.values())
                TOKENS.put(type.value, type);
        }

        public final int value;

        TokenType(int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }

        public static TokenType fromValue(int val)
        {
            return TOKENS.get(val);
        }
    }

    private AbstractType validator;

    private StandardTokenizerInterface scanner;
    private StandardTokenizerOptions options;
    private FilterPipelineTask filterPipeline;

    protected Reader inputReader = null;

    public String getToken()
    {
        return scanner.getText();
    }

    public final boolean incrementToken() throws IOException
    {
        while(true)
        {
            TokenType currentTokenType = TokenType.fromValue(scanner.getNextToken());
            if (currentTokenType == TokenType.EOF)
                return false;
            if (scanner.yylength() <= options.getMaxTokenLength()
                    && scanner.yylength() >= options.getMinTokenLength())
                return true;
        }
    }

    protected String getFilteredCurrentToken() throws IOException
    {
        String token = getToken();
        Object pipelineRes;

        while (true)
        {
            pipelineRes = FilterPipelineExecutor.execute(filterPipeline, token);
            if (pipelineRes != null)
                break;

            boolean reachedEOF = incrementToken();
            if (!reachedEOF)
                break;

            token = getToken();
        }

        return (String) pipelineRes;
    }

    private FilterPipelineTask getFilterPipeline()
    {
        FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
        if (!options.isCaseSensitive() && options.shouldLowerCaseTerms())
            builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
        if (!options.isCaseSensitive() && options.shouldUpperCaseTerms())
            builder = builder.add("to_upper", new BasicResultFilters.UpperCase());
        if (options.shouldIgnoreStopTerms())
            builder = builder.add("skip_stop_words", new StopWordFilters.DefaultStopWordFilter(options.getLocale()));
        if (options.shouldStemTerms())
            builder = builder.add("term_stemming", new StemmingFilters.DefaultStemmingFilter(options.getLocale()));
        return builder.build();
    }

    public void init(Map<String, String> options, AbstractType validator)
    {
        init(StandardTokenizerOptions.buildFromMap(options), validator);
    }

    @VisibleForTesting
    protected void init(StandardTokenizerOptions options)
    {
        init(options, UTF8Type.instance);
    }

    public void init(StandardTokenizerOptions tokenizerOptions, AbstractType validator)
    {
        this.validator = validator;
        this.options = tokenizerOptions;
        this.filterPipeline = getFilterPipeline();

        Reader reader = new InputStreamReader(new DataInputBuffer(ByteBufferUtil.EMPTY_BYTE_BUFFER, false), StandardCharsets.UTF_8);
        this.scanner = new StandardTokenizerImpl(reader);
        this.inputReader = reader;
    }

    public boolean hasNext()
    {
        try
        {
            if (incrementToken())
            {
                if (getFilteredCurrentToken() != null)
                {
                    this.next = validator.fromString(normalize(getFilteredCurrentToken()));
                    return true;
                }
            }
        }
        catch (IOException e)
        {}

        return false;
    }

    public void reset(ByteBuffer input)
    {
        this.next = null;
        Reader reader = new InputStreamReader(new DataInputBuffer(input, false), StandardCharsets.UTF_8);
        scanner.yyreset(reader);
        this.inputReader = reader;
    }

    @VisibleForTesting
    public void reset(InputStream input)
    {
        this.next = null;
        Reader reader = new InputStreamReader(input, StandardCharsets.UTF_8);
        scanner.yyreset(reader);
        this.inputReader = reader;
    }

    public boolean isTokenizing()
    {
        return true;
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> validator)
    {
        return VALID_ANALYZABLE_TYPES.contains(validator);
    }
}
