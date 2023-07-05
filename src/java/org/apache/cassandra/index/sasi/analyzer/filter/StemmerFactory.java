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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.cassandra.concurrent.ImmediateExecutor;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.DanishStemmer;
import org.tartarus.snowball.ext.DutchStemmer;
import org.tartarus.snowball.ext.EnglishStemmer;
import org.tartarus.snowball.ext.FinnishStemmer;
import org.tartarus.snowball.ext.FrenchStemmer;
import org.tartarus.snowball.ext.GermanStemmer;
import org.tartarus.snowball.ext.HungarianStemmer;
import org.tartarus.snowball.ext.ItalianStemmer;
import org.tartarus.snowball.ext.NorwegianStemmer;
import org.tartarus.snowball.ext.PortugueseStemmer;
import org.tartarus.snowball.ext.RomanianStemmer;
import org.tartarus.snowball.ext.RussianStemmer;
import org.tartarus.snowball.ext.SpanishStemmer;
import org.tartarus.snowball.ext.SwedishStemmer;
import org.tartarus.snowball.ext.TurkishStemmer;

/**
 * Returns a SnowballStemmer instance appropriate for
 * a given language
 */
public class StemmerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(StemmerFactory.class);
    private static final LoadingCache<Class, Constructor<?>> STEMMER_CONSTRUCTOR_CACHE = Caffeine.newBuilder()
            .executor(ImmediateExecutor.INSTANCE)
            .build(new CacheLoader<Class, Constructor<?>>()
            {
                public Constructor<?> load(Class aClass) throws Exception
                {
                    try
                    {
                        return aClass.getConstructor();
                    }
                    catch (Exception e) 
                    {
                        logger.error("Failed to get stemmer constructor", e);
                    }
                    return null;
                }
            });

    private static final Map<String, Class> SUPPORTED_LANGUAGES;

    static
    {
        SUPPORTED_LANGUAGES = new HashMap<>();
        SUPPORTED_LANGUAGES.put("de", GermanStemmer.class);
        SUPPORTED_LANGUAGES.put("da", DanishStemmer.class);
        SUPPORTED_LANGUAGES.put("es", SpanishStemmer.class);
        SUPPORTED_LANGUAGES.put("en", EnglishStemmer.class);
        SUPPORTED_LANGUAGES.put("fl", FinnishStemmer.class);
        SUPPORTED_LANGUAGES.put("fr", FrenchStemmer.class);
        SUPPORTED_LANGUAGES.put("hu", HungarianStemmer.class);
        SUPPORTED_LANGUAGES.put("it", ItalianStemmer.class);
        SUPPORTED_LANGUAGES.put("nl", DutchStemmer.class);
        SUPPORTED_LANGUAGES.put("no", NorwegianStemmer.class);
        SUPPORTED_LANGUAGES.put("pt", PortugueseStemmer.class);
        SUPPORTED_LANGUAGES.put("ro", RomanianStemmer.class);
        SUPPORTED_LANGUAGES.put("ru", RussianStemmer.class);
        SUPPORTED_LANGUAGES.put("sv", SwedishStemmer.class);
        SUPPORTED_LANGUAGES.put("tr", TurkishStemmer.class);
    }

    public static SnowballStemmer getStemmer(Locale locale)
    {
        if (locale == null)
            return null;

        String rootLang = locale.getLanguage().substring(0, 2);
        try
        {
            Class clazz = SUPPORTED_LANGUAGES.get(rootLang);
            if(clazz == null)
                return null;
            Constructor<?> ctor = STEMMER_CONSTRUCTOR_CACHE.get(clazz);
            return (SnowballStemmer) ctor.newInstance();
        }
        catch (Exception e)
        {
            logger.debug("Failed to create new SnowballStemmer instance " +
                    "for language [{}]", locale.getLanguage(), e);
        }
        return null;
    }
}
