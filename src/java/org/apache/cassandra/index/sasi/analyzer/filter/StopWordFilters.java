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
package org.apache.cassandra.index.sasi.analyzer.filter;

import java.util.Locale;
import java.util.Set;

/**
 * Filter implementations for input matching Stop Words
 */
public class StopWordFilters
{
    public static class DefaultStopWordFilter extends FilterPipelineTask<String, String>
    {
        private Set<String> stopWords = null;

        public DefaultStopWordFilter(Locale locale)
        {
            this.stopWords = StopWordFactory.getStopWordsForLanguage(locale);
        }

        public String process(String input) throws Exception
        {
            return (stopWords != null && stopWords.contains(input)) ? null : input;
        }
    }
}
