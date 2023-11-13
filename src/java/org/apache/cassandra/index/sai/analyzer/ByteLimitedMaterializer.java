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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Utility class for conditionally materializing a list of tokens given an analyzer and the term to be analyzed.
 * If the cumulative size of the analyzed term exceeds the configured maximum, an empty list is returned to prevent
 * excessive memory/disk usage for a single term.
 */
public class ByteLimitedMaterializer
{
    private static final Logger logger = LoggerFactory.getLogger(ByteLimitedMaterializer.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    public static final String ANALYZED_TERM_OVERSIZE_MESSAGE = "Cannot add term's analyzed tokens of column {} to index" +
                                                                " for key: {}, analzyed term size {}, but max allowed size {}.";

    /**
     * Using the configured analyzer, materialize the tokens for the given term. If the cumulative size of the analyzed
     * term exceeds the configured maximum, an empty list is returned to prevent excessive memory/disk usage for a
     * single term. If the analyzer does not transform the value, the analyzer's byte limit is ignored because indexes
     * already have a limit on the size of a single term.
     * @param analyzer the analyzer to use
     * @param term the term to analyze
     * @param indexContext the index context used for logging
     * @param primaryKey the primary key of the row being indexed
     * @return all the terms produced by the analyzer, or an empty list if the cumulative size of the analyzed term
     * exceeds the configured maximum
     */
    public static List<ByteBuffer> materializeTokens(AbstractAnalyzer analyzer, ByteBuffer term, IndexContext indexContext, PrimaryKey primaryKey)
    {
        try
        {
            analyzer.reset(term);
            if (!analyzer.transformValue())
                return analyzer.hasNext() ? List.of(analyzer.next()) : List.of();

            List<ByteBuffer> tokens = new ArrayList<>();
            int bytesCount = 0;
            while (analyzer.hasNext())
            {
                final ByteBuffer token = analyzer.next();
                tokens.add(token);
                bytesCount += token.remaining();
                if (bytesCount >= IndexContext.MAX_ANALYZED_SIZE)
                {
                    noSpamLogger.warn(indexContext.logMessage(ANALYZED_TERM_OVERSIZE_MESSAGE),
                                      indexContext.getColumnName(),
                                      indexContext.keyValidator().getString(primaryKey.partitionKey().getKey()),
                                      FBUtilities.prettyPrintMemory(bytesCount),
                                      FBUtilities.prettyPrintMemory(IndexContext.MAX_ANALYZED_SIZE));
                    return List.of();
                }
            }
            return tokens;
        }
        finally
        {
            analyzer.end();
        }
    }
}
