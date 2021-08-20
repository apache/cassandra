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

public class NonTokenizingOptions
{
    public static final String NORMALIZE_LOWERCASE = "normalize_lowercase";
    public static final String NORMALIZE_UPPERCASE = "normalize_uppercase";
    public static final String CASE_SENSITIVE = "case_sensitive";

    private boolean caseSensitive;
    private boolean upperCaseOutput;
    private boolean lowerCaseOutput;

    public boolean isCaseSensitive()
    {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive)
    {
        this.caseSensitive = caseSensitive;
    }

    public boolean shouldUpperCaseOutput()
    {
        return upperCaseOutput;
    }

    public void setUpperCaseOutput(boolean upperCaseOutput)
    {
        this.upperCaseOutput = upperCaseOutput;
    }

    public boolean shouldLowerCaseOutput()
    {
        return lowerCaseOutput;
    }

    public void setLowerCaseOutput(boolean lowerCaseOutput)
    {
        this.lowerCaseOutput = lowerCaseOutput;
    }

    public static class OptionsBuilder
    {
        private boolean caseSensitive = true;
        private boolean upperCaseOutput = false;
        private boolean lowerCaseOutput = false;

        public OptionsBuilder()
        {
        }

        public OptionsBuilder caseSensitive(boolean caseSensitive)
        {
            this.caseSensitive = caseSensitive;
            return this;
        }

        public OptionsBuilder upperCaseOutput(boolean upperCaseOutput)
        {
            this.upperCaseOutput = upperCaseOutput;
            return this;
        }

        public OptionsBuilder lowerCaseOutput(boolean lowerCaseOutput)
        {
            this.lowerCaseOutput = lowerCaseOutput;
            return this;
        }

        public NonTokenizingOptions build()
        {
            if (lowerCaseOutput && upperCaseOutput)
                throw new IllegalArgumentException("Options to normalize terms cannot be " +
                        "both uppercase and lowercase at the same time");

            NonTokenizingOptions options = new NonTokenizingOptions();
            options.setCaseSensitive(caseSensitive);
            options.setUpperCaseOutput(upperCaseOutput);
            options.setLowerCaseOutput(lowerCaseOutput);
            return options;
        }
    }

    public static NonTokenizingOptions buildFromMap(Map<String, String> optionsMap)
    {
        OptionsBuilder optionsBuilder = new OptionsBuilder();

        for (Map.Entry<String, String> entry : optionsMap.entrySet())
        {
            switch (entry.getKey())
            {
                case NORMALIZE_LOWERCASE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.lowerCaseOutput(bool);
                    break;
                }
                case NORMALIZE_UPPERCASE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.upperCaseOutput(bool);
                    break;
                }
                case CASE_SENSITIVE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.caseSensitive(bool);
                    break;
                }
            }
        }
        return optionsBuilder.build();
    }

    public static NonTokenizingOptions getDefaultOptions()
    {
        return new OptionsBuilder()
                .caseSensitive(true).lowerCaseOutput(false)
                .upperCaseOutput(false)
                .build();
    }
}
