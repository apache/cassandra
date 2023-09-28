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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.slf4j.Logger;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.lucene.analysis.Analyzer;

public abstract class AbstractAnalyzer implements Iterator<ByteBuffer>
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractAnalyzer.class);

    public static final Set<AbstractType<?>> ANALYZABLE_TYPES = ImmutableSet.of(UTF8Type.instance, AsciiType.instance);

    protected ByteBuffer next = null;
    String nextLiteral = null;

    /**
     * @return true if index value is transformed, eg. normalized or lower-cased or tokenized.
     */
    public abstract boolean transformValue();

    /**
     * Call when tokenization is finished.  Used by the LuceneAnalyzer.
     */
    public void end()
    {
    }

    /**
     * Note: This method does not advance, as we rely on {@link #hasNext()} to buffer the next value.
     *
     * @return the raw value currently buffered by this iterator
     */
    @Override
    public ByteBuffer next()
    {
        if (next == null)
            throw new NoSuchElementException();
        return next;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    protected abstract void resetInternal(ByteBuffer input);

    public void reset(ByteBuffer input)
    {
        this.next = null;
        this.nextLiteral = null;

        resetInternal(input);
    }

    public static boolean hasQueryAnalyzer(Map<String, String> options)
    {
       return options.containsKey(LuceneAnalyzer.QUERY_ANALYZER);
    }

    public interface AnalyzerFactory extends Closeable
    {
        AbstractAnalyzer create();

        default void close()
        {
        }
    }

    public static AnalyzerFactory fromOptionsQueryAnalyzer(final AbstractType<?> type, final Map<String, String> options)
    {
        final String json = options.get(LuceneAnalyzer.QUERY_ANALYZER);
        return toAnalyzerFactory(json, type, options);
    }

    public static AnalyzerFactory toAnalyzerFactory(String json, final AbstractType<?> type, final Map<String, String> options) //throws Exception
    {
        try
        {
            final Analyzer analyzer = JSONAnalyzerParser.parse(json);
            return new AnalyzerFactory()
            {
                @Override
                public void close()
                {
                    analyzer.close();
                }

                public AbstractAnalyzer create()
                {
                    return new LuceneAnalyzer(type, analyzer, options);
                }
            };
        }
        catch (InvalidRequestException ex)
        {
            throw ex;
        }
        catch (Exception ex)
        {
            throw new InvalidRequestException("CQL type " + type.asCQL3Type() + " cannot be analyzed options="+options, ex);
        }
    }

    public static boolean isAnalyzed(Map<String, String> options) {
        return options.containsKey(LuceneAnalyzer.INDEX_ANALYZER) || NonTokenizingOptions.hasNonDefaultOptions(options);
    }

    public static AnalyzerFactory fromOptions(AbstractType<?> type, Map<String, String> options)
    {
        boolean containsIndexAnalyzer = options.containsKey(LuceneAnalyzer.INDEX_ANALYZER);
        boolean containsNonTokenizingOptions = NonTokenizingOptions.hasNonDefaultOptions(options);
        if (containsIndexAnalyzer && containsNonTokenizingOptions)
        {
            logger.warn("Invalid combination of options for index_analyzer: {}", options);
            var optionsToStrip = List.of(NonTokenizingOptions.CASE_SENSITIVE, NonTokenizingOptions.NORMALIZE, NonTokenizingOptions.ASCII);
            options = Maps.filterKeys(options, k -> !optionsToStrip.contains(k));
            logger.warn("Rewrote options to {}", options);
        }
        boolean containsQueryAnalyzer = options.containsKey(LuceneAnalyzer.QUERY_ANALYZER);
        if (containsQueryAnalyzer && !containsIndexAnalyzer && !containsNonTokenizingOptions)
        {
            throw new InvalidRequestException("Cannot specify query_analyzer without an index_analyzer option or any" +
                                              " combination of case_sensitive, normalize, or ascii options. options=" + options);
        }

        if (containsIndexAnalyzer)
        {
            String json = options.get(LuceneAnalyzer.INDEX_ANALYZER);
            return toAnalyzerFactory(json, type, options);
        }

        if (containsNonTokenizingOptions)
        {
            if (TypeUtil.isIn(type, ANALYZABLE_TYPES))
            {
                // load NonTokenizingAnalyzer so it'll validate options
                NonTokenizingAnalyzer a = new NonTokenizingAnalyzer(type, options);
                a.end();
                Map<String, String> finalOptions = options;
                return () -> new NonTokenizingAnalyzer(type, finalOptions);
            }
            else
            {
                throw new InvalidRequestException("CQL type " + type.asCQL3Type() + " cannot be analyzed.");
            }
        }
        return NoOpAnalyzer::new;
    }
}
