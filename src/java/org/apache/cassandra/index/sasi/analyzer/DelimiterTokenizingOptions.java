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

import java.util.Map;

/** Simple tokenizer based on a specified delimiter (rather than whitespace).
 */
public class DelimiterTokenizingOptions
{
    public static final String DELIMITER = "delimiter";

    private final char delimiter;

    private DelimiterTokenizingOptions(char delimiter)
    {
        this.delimiter = delimiter;
    }

    char getDelimiter()
    {
        return delimiter;
    }

    private static class OptionsBuilder
    {
        private char delimiter = ',';

        public DelimiterTokenizingOptions build()
        {
            return new DelimiterTokenizingOptions(delimiter);
        }
    }

    static DelimiterTokenizingOptions buildFromMap(Map<String, String> optionsMap)
    {
        OptionsBuilder optionsBuilder = new OptionsBuilder();

        for (Map.Entry<String, String> entry : optionsMap.entrySet())
        {
            switch (entry.getKey())
            {
                case DELIMITER:
                {
                    String value = entry.getValue();
                    if (1 != value.length())
                        throw new IllegalArgumentException(String.format("Only single character delimiters supported, was %s", value));

                    optionsBuilder.delimiter = entry.getValue().charAt(0);
                    break;
                }
            }
        }
        return optionsBuilder.build();
    }
}
