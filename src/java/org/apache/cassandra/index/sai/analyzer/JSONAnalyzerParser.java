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

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.analyzer.filter.BuiltInAnalyzers;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;

public class JSONAnalyzerParser
{
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static Analyzer parse(String json) throws IOException
    {
        Analyzer analyzer = matchBuiltInAnalzyer(json.toUpperCase());
        if (analyzer != null)
        {
            return analyzer;
        }

        // Don't have built in analyzer, parse JSON
        LuceneCustomAnalyzerConfig analyzerModel = JSON_MAPPER.readValue(json, LuceneCustomAnalyzerConfig.class);

        CustomAnalyzer.Builder builder = CustomAnalyzer.builder(new ArgsStringLoader());
        // An ommitted tokenizer maps directly to the keyword tokenizer, which is an identity map on input terms
        if (analyzerModel.getTokenizer() == null)
        {
            if (analyzerModel.getFilters().isEmpty() && analyzerModel.getCharFilters().isEmpty())
            {
                throw new InvalidRequestException("Analzyer config requires at least a tokenizer, a filter, or a charFilter, but none found. config=" + json);
            }
            builder.withTokenizer("keyword");
        }
        else
        {
            String name = analyzerModel.getTokenizer().getName();
            Map<String, String> args = analyzerModel.getTokenizer().getArgs();
            builder.withTokenizer(name, applyTokenizerDefaults(name, args));
        }
        for (LuceneClassNameAndArgs filter : analyzerModel.getFilters())
        {
            if (filter.getName() == null)
            {
                throw new InvalidRequestException("filter 'name' field is required for options=" + json);
            }
            builder.addTokenFilter(filter.getName(), filter.getArgs());
        }

        for (LuceneClassNameAndArgs charFilter : analyzerModel.getCharFilters())
        {
            if (charFilter.getName() == null)
            {
                throw new InvalidRequestException("charFilter 'name' field is required for options=" + json);
            }
            builder.addCharFilter(charFilter.getName(), charFilter.getArgs());
        }
        return builder.build();
    }

    private static Analyzer matchBuiltInAnalzyer(String maybeAnalyzer) {
        for (BuiltInAnalyzers analyzer : BuiltInAnalyzers.values()) {
            if (analyzer.name().equals(maybeAnalyzer)) {
                return analyzer.getNewAnalyzer();
            }
        }
        return null;
    }

    private static Map<String, String> applyTokenizerDefaults(String filterName, Map<String, String> args)
    {
        if (NGramTokenizerFactory.NAME.equalsIgnoreCase(filterName))
        {
            // Lucene's defaults are 1 and 2 respectively, which has a large memory overhead.
            args.putIfAbsent("minGramSize", "3");
            args.putIfAbsent("maxGramSize", "7");
        }
        return args;
    }
}
