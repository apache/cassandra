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

import java.util.Locale;
import java.util.Map;

/**
 * Various options for controlling tokenization and enabling
 * or disabling features
 */
public class StandardTokenizerOptions
{
    public static final String TOKENIZATION_ENABLE_STEMMING = "tokenization_enable_stemming";
    public static final String TOKENIZATION_SKIP_STOP_WORDS = "tokenization_skip_stop_words";
    public static final String TOKENIZATION_LOCALE = "tokenization_locale";
    public static final String TOKENIZATION_NORMALIZE_LOWERCASE = "tokenization_normalize_lowercase";
    public static final String TOKENIZATION_NORMALIZE_UPPERCASE = "tokenization_normalize_uppercase";

    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;
    public static final int DEFAULT_MIN_TOKEN_LENGTH = 0;

    private boolean stemTerms;
    private boolean ignoreStopTerms;
    private Locale locale;
    private boolean caseSensitive;
    private boolean allTermsToUpperCase;
    private boolean allTermsToLowerCase;
    private int minTokenLength;
    private int maxTokenLength;

    public boolean shouldStemTerms()
    {
        return stemTerms;
    }

    public void setStemTerms(boolean stemTerms)
    {
        this.stemTerms = stemTerms;
    }

    public boolean shouldIgnoreStopTerms()
    {
        return ignoreStopTerms;
    }

    public void setIgnoreStopTerms(boolean ignoreStopTerms)
    {
        this.ignoreStopTerms = ignoreStopTerms;
    }

    public Locale getLocale()
    {
        return locale;
    }

    public void setLocale(Locale locale)
    {
        this.locale = locale;
    }

    public boolean isCaseSensitive()
    {
        return caseSensitive;
    }

    public void setCaseSensitive(boolean caseSensitive)
    {
        this.caseSensitive = caseSensitive;
    }

    public boolean shouldUpperCaseTerms()
    {
        return allTermsToUpperCase;
    }

    public void setAllTermsToUpperCase(boolean allTermsToUpperCase)
    {
        this.allTermsToUpperCase = allTermsToUpperCase;
    }

    public boolean shouldLowerCaseTerms()
    {
        return allTermsToLowerCase;
    }

    public void setAllTermsToLowerCase(boolean allTermsToLowerCase)
    {
        this.allTermsToLowerCase = allTermsToLowerCase;
    }

    public int getMinTokenLength()
    {
        return minTokenLength;
    }

    public void setMinTokenLength(int minTokenLength)
    {
        this.minTokenLength = minTokenLength;
    }

    public int getMaxTokenLength()
    {
        return maxTokenLength;
    }

    public void setMaxTokenLength(int maxTokenLength)
    {
        this.maxTokenLength = maxTokenLength;
    }

    public static class OptionsBuilder 
    {
        private boolean stemTerms;
        private boolean ignoreStopTerms;
        private Locale locale;
        private boolean caseSensitive;
        private boolean allTermsToUpperCase;
        private boolean allTermsToLowerCase;
        private int minTokenLength = DEFAULT_MIN_TOKEN_LENGTH;
        private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

        public OptionsBuilder()
        {
        }

        public OptionsBuilder stemTerms(boolean stemTerms)
        {
            this.stemTerms = stemTerms;
            return this;
        }

        public OptionsBuilder ignoreStopTerms(boolean ignoreStopTerms)
        {
            this.ignoreStopTerms = ignoreStopTerms;
            return this;
        }

        public OptionsBuilder useLocale(Locale locale)
        {
            this.locale = locale;
            return this;
        }

        public OptionsBuilder caseSensitive(boolean caseSensitive)
        {
            this.caseSensitive = caseSensitive;
            return this;
        }

        public OptionsBuilder alwaysUpperCaseTerms(boolean allTermsToUpperCase)
        {
            this.allTermsToUpperCase = allTermsToUpperCase;
            return this;
        }

        public OptionsBuilder alwaysLowerCaseTerms(boolean allTermsToLowerCase)
        {
            this.allTermsToLowerCase = allTermsToLowerCase;
            return this;
        }

        /**
         * Set the min allowed token length.  Any token shorter
         * than this is skipped.
         */
        public OptionsBuilder minTokenLength(int minTokenLength)
        {
            if (minTokenLength < 1)
                throw new IllegalArgumentException("minTokenLength must be greater than zero");
            this.minTokenLength = minTokenLength;
            return this;
        }

        /**
         * Set the max allowed token length.  Any token longer
         * than this is skipped.
         */
        public OptionsBuilder maxTokenLength(int maxTokenLength)
        {
            if (maxTokenLength < 1)
                throw new IllegalArgumentException("maxTokenLength must be greater than zero");
            this.maxTokenLength = maxTokenLength;
            return this;
        }

        public StandardTokenizerOptions build()
        {
            if(allTermsToLowerCase && allTermsToUpperCase)
                throw new IllegalArgumentException("Options to normalize terms cannot be " +
                        "both uppercase and lowercase at the same time");

            StandardTokenizerOptions options = new StandardTokenizerOptions();
            options.setIgnoreStopTerms(ignoreStopTerms);
            options.setStemTerms(stemTerms);
            options.setLocale(locale);
            options.setCaseSensitive(caseSensitive);
            options.setAllTermsToLowerCase(allTermsToLowerCase);
            options.setAllTermsToUpperCase(allTermsToUpperCase);
            options.setMinTokenLength(minTokenLength);
            options.setMaxTokenLength(maxTokenLength);
            return options;
        }
    }

    public static StandardTokenizerOptions buildFromMap(Map<String, String> optionsMap)
    {
        OptionsBuilder optionsBuilder = new OptionsBuilder();

        for (Map.Entry<String, String> entry : optionsMap.entrySet())
        {
            switch(entry.getKey())
            {
                case TOKENIZATION_ENABLE_STEMMING:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.stemTerms(bool);
                    break;
                }
                case TOKENIZATION_SKIP_STOP_WORDS:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.ignoreStopTerms(bool);
                    break;
                }
                case TOKENIZATION_LOCALE:
                {
                    Locale locale = new Locale(entry.getValue());
                    optionsBuilder = optionsBuilder.useLocale(locale);
                    break;
                }
                case TOKENIZATION_NORMALIZE_UPPERCASE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.alwaysUpperCaseTerms(bool);
                    break;
                }
                case TOKENIZATION_NORMALIZE_LOWERCASE:
                {
                    boolean bool = Boolean.parseBoolean(entry.getValue());
                    optionsBuilder = optionsBuilder.alwaysLowerCaseTerms(bool);
                    break;
                }
                default:
                {
                }
            }
        }
        return optionsBuilder.build();
    }

    public static StandardTokenizerOptions getDefaultOptions()
    {
        return new OptionsBuilder()
                .ignoreStopTerms(true).alwaysLowerCaseTerms(true)
                .stemTerms(false).useLocale(Locale.ENGLISH).build();
    }
}
