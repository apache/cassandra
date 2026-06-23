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

package org.apache.cassandra.management.picocli;

import java.util.Arrays;
import java.util.Objects;

import com.google.common.base.Strings;

import org.apache.cassandra.management.api.OptionMetadata;

import picocli.CommandLine;
import picocli.CommandLine.Model.OptionSpec;

import static org.apache.cassandra.management.ManagementUtils.elementType;
import static org.apache.cassandra.management.ManagementUtils.stripAngleBrackets;

/**
 * Implementation of OptionMetadata that extracts metadata from picocli OptionSpec.
 */
public class PicocliOptionMetadata implements OptionMetadata
{
    private final OptionSpec optionSpec;

    public PicocliOptionMetadata(OptionSpec optionSpec)
    {
        this.optionSpec = optionSpec;
    }

    @Override
    public String[] names()
    {
        return optionSpec.names();
    }

    @Override
    public String description()
    {
        String[] description = optionSpec.description();
        if (description == null || description.length == 0)
            return "";
        return String.join("\n", description);
    }

    @Override
    public Class<?> type()
    {
        return optionSpec.type();
    }

    @Override
    public String defaultValue()
    {
        Object defaultValue = optionSpec.defaultValue();
        if (defaultValue == null)
        {
            if (optionSpec.type() == boolean.class || optionSpec.type() == Boolean.class)
                return "false";
            return "";
        }

        return defaultValue.toString();
    }

    @Override
    public boolean required()
    {
        return optionSpec.required();
    }

    @Override
    public String arity()
    {
        CommandLine.Range arity = optionSpec.arity();
        int min = arity.min();
        int max = arity.max();
        
        if (min == 0 && max == 0)
            return "0";
        
        if (min == max)
            return String.valueOf(min);
        else if (max == Integer.MAX_VALUE)
            return min + "..*";
        else
            return min + ".." + max;
    }

    @Override
    public String paramLabel()
    {
        String paramLabel = optionSpec.paramLabel();
        Object userObject = optionSpec.userObject();

        return Strings.isNullOrEmpty(paramLabel) ?
               ((java.lang.reflect.Field) userObject).getName() :
               stripAngleBrackets(paramLabel);
    }

    /**
     * Check if this is a boolean flag option.
     */
    public boolean isFlag()
    {
        return optionSpec.arity().max() == 0;
    }

    /**
     * Get the underlying OptionSpec for advanced usage.
     */
    public OptionSpec getOptionSpec()
    {
        return optionSpec;
    }

    public Object convertValue(Object value) throws Exception
    {
        TypeConverter<?> customConverter = typeConverter() == null ? null : typeConverter()[0];
        return customConverter == null ?
               TypeConverterRegistry.convertValueBasic(value, type(), elementType(optionSpec)) :
               customConverter.convert(value.toString());
    }

    private TypeConverter<?>[] typeConverter()
    {
        return TypeConverter.createFrom(optionSpec);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PicocliOptionMetadata)) return false;
        PicocliOptionMetadata that = (PicocliOptionMetadata) o;
        return Objects.equals(optionSpec, that.optionSpec);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(optionSpec);
    }

    @Override
    public String toString()
    {
        return "PicocliOptionMetadata{" +
               "names=" + Arrays.toString(names()) +
               '}';
    }
}

