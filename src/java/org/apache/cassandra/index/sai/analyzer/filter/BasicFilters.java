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

package org.apache.cassandra.index.sai.analyzer.filter;

import java.text.Normalizer;
import java.util.Locale;

import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class BasicFilters
{
    private static final Locale DEFAULT_LOCALE = Locale.getDefault();

    public static class LowerCase extends FilterPipeline.Task
    {
        private final Locale locale;

        public LowerCase()
        {
            this.locale = DEFAULT_LOCALE;
        }

        @Override
        public String process(String input)
        {
            return input.toLowerCase(locale);
        }
    }

    public static class Normalize extends FilterPipeline.Task
    {
        public Normalize() { }

        @Override
        public String process(String input)
        {
            if (input == null) return null;
            return Normalizer.isNormalized(input, Normalizer.Form.NFC) ? input : Normalizer.normalize(input, Normalizer.Form.NFC);
        }
    }

    public static class Ascii extends FilterPipeline.Task
    {
        public Ascii() { }

        @Override
        public String process(String input)
        {
            if (input == null) return null;
            char[] inputChars = input.toCharArray();
            // The output can (potentially) be 4 times the size of the input
            char[] outputChars = new char[inputChars.length * 4];
            int outputSize = ASCIIFoldingFilter.foldToASCII(inputChars, 0, outputChars, 0, inputChars.length);
            return new String(outputChars, 0, outputSize);
        }
    }

    public static class NoOperation extends FilterPipeline.Task
    {
        @Override
        public String process(String input)
        {
            return input;
        }
    }
}
