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
package org.apache.cassandra.cql3;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.index.SecondaryIndex;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.SyntaxException;

public class IndexPropDefs extends PropertyDefinitions
{
    public static final String KW_OPTIONS = "options";

    public static final Set<String> keywords = new HashSet<String>();
    public static final Set<String> obsoleteKeywords = new HashSet<String>();

    public static final String INDEX_CLASS_KEY = "class";

    static
    {
        keywords.add(KW_OPTIONS);
    }

    public void validate(boolean isCustom) throws RequestValidationException
    {
        validate(keywords, obsoleteKeywords);
        if (isCustom && !getOptions().containsKey(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME))
            throw new InvalidRequestException(String.format("Custom index requires '%s' option to be specified", INDEX_CLASS_KEY));
        if (!isCustom && !getOptions().isEmpty())
            throw new InvalidRequestException(String.format("Only custom indexes can currently be parametrized"));
    }

    public Map<String, String> getOptions() throws SyntaxException
    {
        Map<String, String> options = getMap(KW_OPTIONS);

        if (options == null)
            return Collections.emptyMap();

        if (!options.isEmpty() && options.containsKey(INDEX_CLASS_KEY))
        {
            options.put(SecondaryIndex.CUSTOM_INDEX_OPTION_NAME, options.get(INDEX_CLASS_KEY));
            options.remove(INDEX_CLASS_KEY);
        }

        return options;
    }
}
