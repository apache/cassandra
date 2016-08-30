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

import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.*;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Returns a SnowballStemmer instance appropriate for
 * a given language
 */
public class StemmerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(StemmerFactory.class);
    private static final LoadingCache<Class, Constructor<?>> STEMMER_CONSTRUCTOR_CACHE = CacheBuilder.newBuilder()
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
        SUPPORTED_LANGUAGES.put("de", germanStemmer.class);
        SUPPORTED_LANGUAGES.put("da", danishStemmer.class);
        SUPPORTED_LANGUAGES.put("es", spanishStemmer.class);
        SUPPORTED_LANGUAGES.put("en", englishStemmer.class);
        SUPPORTED_LANGUAGES.put("fl", finnishStemmer.class);
        SUPPORTED_LANGUAGES.put("fr", frenchStemmer.class);
        SUPPORTED_LANGUAGES.put("hu", hungarianStemmer.class);
        SUPPORTED_LANGUAGES.put("it", italianStemmer.class);
        SUPPORTED_LANGUAGES.put("nl", dutchStemmer.class);
        SUPPORTED_LANGUAGES.put("no", norwegianStemmer.class);
        SUPPORTED_LANGUAGES.put("pt", portugueseStemmer.class);
        SUPPORTED_LANGUAGES.put("ro", romanianStemmer.class);
        SUPPORTED_LANGUAGES.put("ru", russianStemmer.class);
        SUPPORTED_LANGUAGES.put("sv", swedishStemmer.class);
        SUPPORTED_LANGUAGES.put("tr", turkishStemmer.class);
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
