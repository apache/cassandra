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

import org.apache.cassandra.management.api.ParameterMetadata;

import picocli.CommandLine;
import picocli.CommandLine.Model.PositionalParamSpec;

import static org.apache.cassandra.management.ManagementUtils.elementType;
import static org.apache.cassandra.management.ManagementUtils.stripAngleBrackets;

/**
 * Implementation of ParameterMetadata that extracts metadata from picocli PositionalParamSpec.
 */
public class PicocliParameterMetadata implements ParameterMetadata
{
    private final PositionalParamSpec positionalParamSpec;

    public PicocliParameterMetadata(PositionalParamSpec positionalParamSpec)
    {
        this.positionalParamSpec = positionalParamSpec;
    }

    @Override
    public String[] names()
    {
        String name;
        String paramLabel = positionalParamSpec.paramLabel();
        Object userObject = positionalParamSpec.userObject();

        if (paramLabel != null && !paramLabel.isEmpty())
            name = stripAngleBrackets(paramLabel);
        else if (userObject instanceof java.lang.reflect.Field)
            name = ((java.lang.reflect.Field) userObject).getName();
        else
            name = COMMAND_POSITIONAL_PARAM_PREFIX + index();

        return new String[] { name };
    }

    @Override
    public String paramLabel()
    {
        return stripAngleBrackets(positionalParamSpec.paramLabel());
    }

    @Override
    public String description()
    {
        String[] description = positionalParamSpec.description();
        if (description == null || description.length == 0)
            return "";
        return String.join("\n", description);
    }

    @Override
    public Class<?> type()
    {
        return positionalParamSpec.type();
    }

    @Override
    public String defaultValue()
    {
        return "";
    }

    @Override
    public int index()
    {
        return positionalParamSpec.index().min();
    }

    @Override
    public boolean required()
    {
        return positionalParamSpec.arity().min() > 0;
    }

    @Override
    public String arity()
    {
        CommandLine.Range arity = positionalParamSpec.arity();
        int min = arity.min();
        int max = arity.max();
        
        if (min == max)
            return String.valueOf(min);
        else if (max == Integer.MAX_VALUE)
            return min + "..*";
        else
            return min + ".." + max;
    }

    /**
     * Get the underlying PositionalParamSpec for advanced usage.
     */
    public PositionalParamSpec getPositionalParamSpec()
    {
        return positionalParamSpec;
    }

    public Object convertValue(Object value) throws Exception
    {
        TypeConverter<?> customConverter = typeConverter() == null ? null : typeConverter()[0];
        return customConverter == null ?
               TypeConverterRegistry.convertValueBasic(value, type(), elementType(positionalParamSpec)) :
               customConverter.convert(value.toString());
    }

    private TypeConverter<?>[] typeConverter()
    {
        return TypeConverter.createFrom(positionalParamSpec);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PicocliParameterMetadata)) return false;
        PicocliParameterMetadata that = (PicocliParameterMetadata) o;
        return Objects.equals(positionalParamSpec, that.positionalParamSpec);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(positionalParamSpec);
    }

    @Override
    public String toString()
    {
        return "PicocliParameterMetadata{" +
               "names=" + Arrays.toString(names()) +
               '}';
    }
}

