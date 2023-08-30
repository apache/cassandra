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

import org.tartarus.snowball.SnowballStemmer;

/**
 * Filters for performing Stemming on tokens
 */
public class StemmingFilters
{
    public static class DefaultStemmingFilter extends FilterPipelineTask<String, String>
    {
        private SnowballStemmer stemmer;

        public DefaultStemmingFilter(Locale locale)
        {
            stemmer = StemmerFactory.getStemmer(locale);
        }

        public String process(String input) throws Exception
        {
            if (input == null || stemmer == null)
                return input;
            stemmer.setCurrent(input);
            return (stemmer.stem()) ? stemmer.getCurrent() : input;
        }
    }
}
