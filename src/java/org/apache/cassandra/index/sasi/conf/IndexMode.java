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
package org.apache.cassandra.index.sasi.conf;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.NoOpAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer;
import org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder.Mode;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.sasi.plan.Expression.Op;
import org.apache.cassandra.schema.IndexMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMode
{
    private static final Logger logger = LoggerFactory.getLogger(IndexMode.class);

    public static final IndexMode NOT_INDEXED = new IndexMode(Mode.PREFIX, true, false, NonTokenizingAnalyzer.class, 0);

    private static final Set<AbstractType<?>> TOKENIZABLE_TYPES = new HashSet<AbstractType<?>>()
    {{
        add(UTF8Type.instance);
        add(AsciiType.instance);
    }};

    private static final String INDEX_MODE_OPTION = "mode";
    private static final String INDEX_ANALYZED_OPTION = "analyzed";
    private static final String INDEX_ANALYZER_CLASS_OPTION = "analyzer_class";
    private static final String INDEX_IS_LITERAL_OPTION = "is_literal";
    private static final String INDEX_MAX_FLUSH_MEMORY_OPTION = "max_compaction_flush_memory_in_mb";
    private static final double INDEX_MAX_FLUSH_DEFAULT_MULTIPLIER = 0.15;
    private static final long DEFAULT_MAX_MEM_BYTES = (long) (1073741824 * INDEX_MAX_FLUSH_DEFAULT_MULTIPLIER); // 1G default for memtable

    public final Mode mode;
    public final boolean isAnalyzed, isLiteral;
    public final Class analyzerClass;
    public final long maxCompactionFlushMemoryInBytes;

    private IndexMode(Mode mode, boolean isLiteral, boolean isAnalyzed, Class analyzerClass, long maxMemBytes)
    {
        this.mode = mode;
        this.isLiteral = isLiteral;
        this.isAnalyzed = isAnalyzed;
        this.analyzerClass = analyzerClass;
        this.maxCompactionFlushMemoryInBytes = maxMemBytes;
    }

    public AbstractAnalyzer getAnalyzer(AbstractType<?> validator)
    {
        AbstractAnalyzer analyzer = new NoOpAnalyzer();

        try
        {
            if (isAnalyzed)
            {
                if (analyzerClass != null)
                    analyzer = (AbstractAnalyzer) analyzerClass.newInstance();
                else if (TOKENIZABLE_TYPES.contains(validator))
                    analyzer = new StandardAnalyzer();
            }
        }
        catch (InstantiationException | IllegalAccessException e)
        {
            logger.error("Failed to create new instance of analyzer with class [{}]", analyzerClass.getName(), e);
        }

        return analyzer;
    }

    public static void validateAnalyzer(Map<String, String> indexOptions, ColumnMetadata cd) throws ConfigurationException
    {
        // validate that a valid analyzer class was provided if specified
        if (indexOptions.containsKey(INDEX_ANALYZER_CLASS_OPTION))
        {
            Class<?> analyzerClass;
            try
            {
                analyzerClass = Class.forName(indexOptions.get(INDEX_ANALYZER_CLASS_OPTION));
            }
            catch (ClassNotFoundException e)
            {
                throw new ConfigurationException(String.format("Invalid analyzer class option specified [%s]",
                                                               indexOptions.get(INDEX_ANALYZER_CLASS_OPTION)));
            }

            AbstractAnalyzer analyzer;
            try
            {
                analyzer = (AbstractAnalyzer) analyzerClass.newInstance();
                analyzer.validate(indexOptions, cd);
            }
            catch (InstantiationException | IllegalAccessException e)
            {
                throw new ConfigurationException(String.format("Unable to initialize analyzer class option specified [%s]",
                                                               analyzerClass.getSimpleName()));
            }
        }
    }

    public static IndexMode getMode(ColumnMetadata column, Optional<IndexMetadata> config) throws ConfigurationException
    {
        return getMode(column, config.isPresent() ? config.get().options : null);
    }

    public static IndexMode getMode(ColumnMetadata column, Map<String, String> indexOptions) throws ConfigurationException
    {
        if (indexOptions == null || indexOptions.isEmpty())
            return IndexMode.NOT_INDEXED;

        Mode mode;

        try
        {
            mode = indexOptions.get(INDEX_MODE_OPTION) == null
                            ? Mode.PREFIX
                            : Mode.mode(indexOptions.get(INDEX_MODE_OPTION));
        }
        catch (IllegalArgumentException e)
        {
            throw new ConfigurationException("Incorrect index mode: " + indexOptions.get(INDEX_MODE_OPTION));
        }

        boolean isAnalyzed = false;
        Class analyzerClass = null;
        try
        {
            if (indexOptions.get(INDEX_ANALYZER_CLASS_OPTION) != null)
            {
                analyzerClass = Class.forName(indexOptions.get(INDEX_ANALYZER_CLASS_OPTION));
                isAnalyzed = indexOptions.get(INDEX_ANALYZED_OPTION) == null
                              ? true : Boolean.parseBoolean(indexOptions.get(INDEX_ANALYZED_OPTION));
            }
            else if (indexOptions.get(INDEX_ANALYZED_OPTION) != null)
            {
                isAnalyzed = Boolean.parseBoolean(indexOptions.get(INDEX_ANALYZED_OPTION));
            }
        }
        catch (ClassNotFoundException e)
        {
            // should not happen as we already validated we could instantiate an instance in validateAnalyzer()
            logger.error("Failed to find specified analyzer class [{}]. Falling back to default analyzer",
                         indexOptions.get(INDEX_ANALYZER_CLASS_OPTION));
        }

        boolean isLiteral = false;
        try
        {
            String literalOption = indexOptions.get(INDEX_IS_LITERAL_OPTION);
            AbstractType<?> validator = column.cellValueType();

            isLiteral = literalOption == null
                            ? (validator instanceof UTF8Type || validator instanceof AsciiType)
                            : Boolean.parseBoolean(literalOption);
        }
        catch (Exception e)
        {
            logger.error("failed to parse {} option, defaulting to 'false'.", INDEX_IS_LITERAL_OPTION);
        }

        long maxMemBytes = indexOptions.get(INDEX_MAX_FLUSH_MEMORY_OPTION) == null
                ? DEFAULT_MAX_MEM_BYTES
                : 1048576L * Long.parseLong(indexOptions.get(INDEX_MAX_FLUSH_MEMORY_OPTION));

        if (maxMemBytes > 100L * 1073741824)
        {
            logger.error("{} configured as {} is above 100GiB, reverting to default 1GB", INDEX_MAX_FLUSH_MEMORY_OPTION, maxMemBytes);
            maxMemBytes = DEFAULT_MAX_MEM_BYTES;
        }
        return new IndexMode(mode, isLiteral, isAnalyzed, analyzerClass, maxMemBytes);
    }

    public boolean supports(Op operator)
    {
        return mode.supports(operator);
    }
}
