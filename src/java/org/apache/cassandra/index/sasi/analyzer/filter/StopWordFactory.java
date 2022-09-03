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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletionException;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.apache.cassandra.io.util.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a list of Stop Words for a given language
 */
public class StopWordFactory
{
    private static final Logger logger = LoggerFactory.getLogger(StopWordFactory.class);

    private static final String DEFAULT_RESOURCE_EXT = "_ST.txt";
    private static final String DEFAULT_RESOURCE_PREFIX = StopWordFactory.class.getPackage()
            .getName().replace(".", File.pathSeparator());
    private static final Set<String> SUPPORTED_LANGUAGES = new HashSet<>(
            Arrays.asList("ar","bg","cs","de","en","es","fi","fr","hi","hu","it",
            "pl","pt","ro","ru","sv"));

    private static final LoadingCache<String, Set<String>> STOP_WORDS_CACHE = Caffeine.newBuilder()
            .executor(ImmediateExecutor.INSTANCE)
            .build(StopWordFactory::getStopWordsFromResource);

    public static Set<String> getStopWordsForLanguage(Locale locale)
    {
        if (locale == null)
            return null;

        String rootLang = locale.getLanguage().substring(0, 2);
        try
        {
            return (!SUPPORTED_LANGUAGES.contains(rootLang)) ? null : STOP_WORDS_CACHE.get(rootLang);
        }
        catch (CompletionException e)
        {
            logger.error("Failed to populate Stop Words Cache for language [{}]", locale.getLanguage(), e);
            return null;
        }
    }

    private static Set<String> getStopWordsFromResource(String language)
    {
        Set<String> stopWords = new HashSet<>();
        String resourceName = DEFAULT_RESOURCE_PREFIX + File.pathSeparator() + language + DEFAULT_RESOURCE_EXT;
        try (InputStream is = StopWordFactory.class.getClassLoader().getResourceAsStream(resourceName);
             BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)))
        {
                String line;
                while ((line = r.readLine()) != null)
                {
                    //skip comments (lines starting with # char)
                    if(line.charAt(0) == '#')
                        continue;
                    stopWords.add(line.trim());
                }
        }
        catch (Exception e)
        {
            logger.error("Failed to retrieve Stop Terms resource for language [{}]", language, e);
        }
        return stopWords;
    }
}
