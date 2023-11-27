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
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.analyzer.filter.BasicFilters;
import org.apache.cassandra.index.sai.analyzer.filter.FilterPipeline;
import org.apache.cassandra.index.sai.analyzer.filter.FilterPipelineExecutor;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Analyzer that does *not* tokenize the input. Optionally will
 * apply filters for the input based on {@link NonTokenizingOptions}.
 */
public class NonTokenizingAnalyzer extends AbstractAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(NonTokenizingAnalyzer.class);

    private final IndexTermType indexTermType;
    private final NonTokenizingOptions options;
    private final FilterPipeline filterPipeline;

    private ByteBuffer input;
    private boolean hasNext = false;

    NonTokenizingAnalyzer(IndexTermType indexTermType, Map<String, String> options)
    {
        this(indexTermType, NonTokenizingOptions.fromMap(options));
    }

    NonTokenizingAnalyzer(IndexTermType indexTermType, NonTokenizingOptions tokenizerOptions)
    {
        this.indexTermType = indexTermType;
        this.options = tokenizerOptions;
        this.filterPipeline = getFilterPipeline();
    }

    @Override
    public boolean hasNext()
    {
        // check that we know how to handle the input, otherwise bail
        if (!indexTermType.isString())
            return false;

        if (hasNext)
        {
            try
            {
                String input = indexTermType.asString(this.input);

                if (input == null)
                {
                    throw new MarshalException(String.format("'null' deserialized value for %s with %s",
                                                             ByteBufferUtil.bytesToHex(this.input), indexTermType));
                }

                String result = FilterPipelineExecutor.execute(filterPipeline, input);
                
                if (result == null)
                {
                    nextLiteral = null;
                    next = null;
                    return false;
                }

                nextLiteral = result;
                next = indexTermType.fromString(result);

                return true;
            }
            catch (MarshalException e)
            {
                logger.error("Failed to deserialize value with " + indexTermType, e);
                return false;
            }
            finally
            {
                hasNext = false;
            }
        }

        return false;
    }

    @Override
    public boolean transformValue()
    {
        return !options.isCaseSensitive() || options.isNormalized() || options.isAscii();
    }

    @Override
    protected void resetInternal(ByteBuffer input)
    {
        this.input = input;
        this.hasNext = true;
    }

    private FilterPipeline getFilterPipeline()
    {
        FilterPipeline builder = new FilterPipeline(new BasicFilters.NoOperation());
        
        if (!options.isCaseSensitive())
            builder = builder.add("to_lower", new BasicFilters.LowerCase());
        
        if (options.isNormalized())
            builder = builder.add("normalize", new BasicFilters.Normalize());

        if (options.isAscii())
            builder = builder.add("ascii", new BasicFilters.Ascii());
        
        return builder;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("caseSensitive", options.isCaseSensitive())
                          .add("normalized", options.isNormalized())
                          .add("ascii", options.isAscii())
                          .toString();
    }
}
