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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.util.ResourceLoader;

/**
 * A resource loader that considers each passed string as the resource. This class allows us to configure stop words
 * and synonyms as arguments in the 'args' parameter of the filter's configuration.
 * Example: WITH OPTIONS = {'index_analyzer':'{"tokenizer":{"name" : "whitespace"}, "filters":[{"name":"stop", "args": {"words": "the, test"}}]}'}
 * The above configuration will create a stop filter with the words "the" and "test". The args key name, e.g. words,
 * is specific to the lucene component being configured. The delimiter can vary based on the filter, but appears to
 * be comma delimited for most lucene components. Note that commas can be escaped with a backslash.
 */
public class ArgsStringLoader implements ResourceLoader
{
    @Override
    public InputStream openResource(String s) throws IOException
    {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
        try {
            return Class.forName(cname).asSubclass(expectedType);
        } catch (Exception e) {
            throw new RuntimeException("Cannot load class: " + cname, e);
        }
    }
}
