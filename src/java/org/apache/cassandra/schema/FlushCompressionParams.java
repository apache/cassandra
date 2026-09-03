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

package org.apache.cassandra.schema;

import java.util.Arrays;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.lang.String.format;

public final class FlushCompressionParams
{
    public final Option configurationKey;

    public enum Option
    {
        auto,
        none,
        fast,
        table;

        private static final String possibleValues = Arrays.toString(Option.values());
    }

    public static final FlushCompressionParams DEFAULT = new FlushCompressionParams(Option.auto);

    public static FlushCompressionParams fromString(String value)
    {
        try
        {
            return new FlushCompressionParams(Option.valueOf(value));
        }
        catch (Throwable t)
        {
            throw new ConfigurationException(format("Invalid value used for flush compression parameter: %s, possible values: %s",
                                                    value,
                                                    Option.possibleValues));
        }
    }

    private FlushCompressionParams(Option configurationKey)
    {
        this.configurationKey = configurationKey;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof FlushCompressionParams))
            return false;

        return configurationKey == ((FlushCompressionParams) o).configurationKey;
    }

    @Override
    public int hashCode()
    {
        return configurationKey.hashCode();
    }

    @Override
    public String toString()
    {
        return configurationKey.toString();
    }
}
