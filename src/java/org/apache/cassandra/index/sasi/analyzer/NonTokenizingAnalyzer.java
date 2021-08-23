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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sasi.analyzer.filter.BasicResultFilters;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineBuilder;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineExecutor;
import org.apache.cassandra.index.sasi.analyzer.filter.FilterPipelineTask;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Analyzer that does *not* tokenize the input. Optionally will
 * apply filters for the input output as defined in analyzers options
 */
public class NonTokenizingAnalyzer extends AbstractAnalyzer
{
    private static final Logger logger = LoggerFactory.getLogger(NonTokenizingAnalyzer.class);

    private static final Set<AbstractType<?>> VALID_ANALYZABLE_TYPES = new HashSet<AbstractType<?>>()
    {{
            add(UTF8Type.instance);
            add(AsciiType.instance);
    }};

    private AbstractType<?> validator;
    private NonTokenizingOptions options;
    private FilterPipelineTask filterPipeline;

    private ByteBuffer input;
    private boolean hasNext = false;

    @Override
    public void validate(Map<String, String> options, ColumnMetadata cm) throws ConfigurationException
    {
        super.validate(options, cm);
        if (options.containsKey(NonTokenizingOptions.CASE_SENSITIVE) &&
            (options.containsKey(NonTokenizingOptions.NORMALIZE_LOWERCASE)
             || options.containsKey(NonTokenizingOptions.NORMALIZE_UPPERCASE)))
            throw new ConfigurationException("case_sensitive option cannot be specified together " +
                                               "with either normalize_lowercase or normalize_uppercase");
    }

    public void init(Map<String, String> options, AbstractType<?> validator)
    {
        init(NonTokenizingOptions.buildFromMap(options), validator);
    }

    public void init(NonTokenizingOptions tokenizerOptions, AbstractType<?> validator)
    {
        this.validator = validator;
        this.options = tokenizerOptions;
        this.filterPipeline = getFilterPipeline();
    }

    public boolean hasNext()
    {
        // check that we know how to handle the input, otherwise bail
        if (!VALID_ANALYZABLE_TYPES.contains(validator))
            return false;

        if (hasNext)
        {
            String inputStr;

            try
            {
                inputStr = validator.getString(input);
                if (inputStr == null)
                    throw new MarshalException(String.format("'null' deserialized value for %s with %s", ByteBufferUtil.bytesToHex(input), validator));

                Object pipelineRes = FilterPipelineExecutor.execute(filterPipeline, inputStr);
                if (pipelineRes == null)
                    return false;

                next = validator.fromString(normalize((String) pipelineRes));
                return true;
            }
            catch (MarshalException e)
            {
                logger.error("Failed to deserialize value with " + validator, e);
                return false;
            }
            finally
            {
                hasNext = false;
            }
        }

        return false;
    }

    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.input = input;
        this.hasNext = true;
    }

    private FilterPipelineTask getFilterPipeline()
    {
        FilterPipelineBuilder builder = new FilterPipelineBuilder(new BasicResultFilters.NoOperation());
        if (options.isCaseSensitive() && options.shouldLowerCaseOutput())
            builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
        if (options.isCaseSensitive() && options.shouldUpperCaseOutput())
            builder = builder.add("to_upper", new BasicResultFilters.UpperCase());
        if (!options.isCaseSensitive())
            builder = builder.add("to_lower", new BasicResultFilters.LowerCase());
        return builder.build();
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> validator)
    {
        return VALID_ANALYZABLE_TYPES.contains(validator);
    }
}
