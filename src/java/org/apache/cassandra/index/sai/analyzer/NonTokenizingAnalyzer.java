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
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.analyzer.filter.BasicResultFilters;
import org.apache.cassandra.index.sai.analyzer.filter.FilterPipelineBuilder;
import org.apache.cassandra.index.sai.analyzer.filter.FilterPipelineExecutor;
import org.apache.cassandra.index.sai.analyzer.filter.FilterPipelineTask;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Analyzer that does *not* tokenize the input. Optionally will
 * apply filters for the input output as defined in analyzers options
 */
public class NonTokenizingAnalyzer extends AbstractAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(NonTokenizingAnalyzer.class);

    private AbstractType<?> type;
    private NonTokenizingOptions options;
    private FilterPipelineTask filterPipeline;

    private ByteBuffer input;
    private boolean hasNext = false;

    NonTokenizingAnalyzer(AbstractType<?> type, Map<String, String> options)
    {
        this(type, NonTokenizingOptions.fromMap(options));
    }

    NonTokenizingAnalyzer(AbstractType<?> type, NonTokenizingOptions tokenizerOptions)
    {
        this.type = type;
        this.options = tokenizerOptions;
        this.filterPipeline = getFilterPipeline();
    }

    @Override
    public boolean hasNext()
    {
        // check that we know how to handle the input, otherwise bail
        if (!TypeUtil.isIn(type, ANALYZABLE_TYPES)) return false;

        if (hasNext)
        {
            try
            {
                String input = type.getString(this.input);

                if (input == null)
                {
                    throw new MarshalException(String.format("'null' deserialized value for %s with %s",
                                                             ByteBufferUtil.bytesToHex(this.input), type));
                }

                String result = FilterPipelineExecutor.execute(filterPipeline, input);
                
                if (result == null)
                {
                    nextLiteral = null;
                    next = null;
                    return false;
                }

                nextLiteral = result;
                next = type.fromString(result);

                return true;
            }
            catch (MarshalException e)
            {
                logger.error("Failed to deserialize value with " + type, e);
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

    private FilterPipelineTask getFilterPipeline()
    {
        FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
        
        if (!options.isCaseSensitive())
        {
            builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
        }
        
        if (options.isNormalized())
        {
            builder = builder.add("normalize", new BasicResultFilters.Normalize());
        }

        if (options.isAscii())
        {
            builder = builder.add("ascii", new BasicResultFilters.Ascii());
        }
        
        return builder.build();
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
