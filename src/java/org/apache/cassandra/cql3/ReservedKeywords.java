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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class ReservedKeywords
{
    private static final String FILE_NAME = "reserved_keywords.txt";

    @VisibleForTesting
    static final Set<String> reservedKeywords = getFromResource();

    private static Set<String> getFromResource()
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        try (InputStream is = ReservedKeywords.class.getResource(FILE_NAME).openConnection().getInputStream();
             BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)))
        {
            String line;
            while ((line = r.readLine()) != null)
            {
                builder.add(line.trim());
            }
        }
        catch (IOException e)
        {
            throw new ConfigurationException(String.format("Unable to read reserved keywords file '%s'", FILE_NAME), e);
        }
        return builder.build();
    }

    public static boolean isReserved(String text)
    {
        return reservedKeywords.contains(text.toUpperCase());
    }
}
