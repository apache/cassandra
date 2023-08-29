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

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LuceneCustomAnalyzerConfig
{
    private final LuceneClassNameAndArgs tokenizer;
    private final List<LuceneClassNameAndArgs> filters;
    private final List<LuceneClassNameAndArgs> charFilters;

    public LuceneCustomAnalyzerConfig(@JsonProperty("tokenizer") LuceneClassNameAndArgs tokenizer,
                                      @JsonProperty("filters") List<LuceneClassNameAndArgs> filters,
                                      @JsonProperty("charFilters") List<LuceneClassNameAndArgs> charFilters)
    {
        this.tokenizer = tokenizer;
        this.filters = filters != null ? filters : List.of();
        this.charFilters = charFilters != null ? charFilters : List.of();
    }

    public LuceneClassNameAndArgs getTokenizer()
    {
        return tokenizer;
    }

    public List<LuceneClassNameAndArgs> getFilters()
    {
        return filters;
    }

    public List<LuceneClassNameAndArgs> getCharFilters()
    {
        return charFilters;
    }
}
