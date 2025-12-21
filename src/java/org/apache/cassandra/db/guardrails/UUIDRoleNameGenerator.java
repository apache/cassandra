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

package org.apache.cassandra.db.guardrails;

import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * Generates random name for user / role.
 * Generator reacts on these parameters when specified in OPTIONS map in CQL:
 * <ul>
 *     <li>name_prefix - arbitrary string to prepend to generated name</li>
 *     <li>name_suffix - arbitraty string to append to generated name</li>
 *     <li>name_size - size of generated string. Can not be less than 10 and more than 32</li>
 * </ul>
 * <p>
 * Additionally, it reacts to this parameter put in cassandra.yaml:
 *
 * <ul>
 *     <li>min_generated_name_size - sets minimum size of name. If {@code name_size} is specified in CQL's OPTIONS,
 *     it will be checked against this minimum size and fail if smaller than that.
 *     </li>
 * </ul>
 * <p>
 * {@code name_prefix} and {@code name_suffix} string lengths are not included in {@code name_size} length.
 * {@code name_size} is only related to generated part of the role name.
 * <p>
 * The first character of generated role name will be always one of {@code a,b,c,d,e,f}.
 */
public class UUIDRoleNameGenerator extends ValueGenerator<String>
{
    public static final String NAME_PREFIX_KEY = "name_prefix";
    public static final String NAME_SUFFIX_KEY = "name_suffix";
    public static final String NAME_SIZE = "name_size";
    public static final String MINIMUM_NAME_SIZE_CONFIG_OPTION = "min_generated_name_size";
    public static final int DEFAULT_MINIMUM_NAME_SIZE = 10;
    private static final Set<String> KEYS_IN_OPTIONS = Set.of(NAME_PREFIX_KEY, NAME_SUFFIX_KEY, NAME_SIZE);

    private static final Pattern PATTERN = Pattern.compile("-");
    public static final char[] FIRST_CHARS = { 'a', 'b', 'c', 'd', 'e', 'f' };

    private static final Random random = new Random();
    // lenght of UUID without hyphens
    // generated name can be longer than this if it has prefix / suffix
    public static final int MAXIMUM_NAME_SIZE = 32;

    private final int minimumNameSize;

    public UUIDRoleNameGenerator()
    {
        this(new CustomGuardrailConfig());
    }

    public UUIDRoleNameGenerator(CustomGuardrailConfig config)
    {
        super(config);
        minimumNameSize = config.resolveInteger(MINIMUM_NAME_SIZE_CONFIG_OPTION, DEFAULT_MINIMUM_NAME_SIZE);
        validateParameters();
    }

    @Override
    public Set<String> getSupportedOptionsKeys()
    {
        return KEYS_IN_OPTIONS;
    }

    @Override
    public String generate(ValueValidator<String> validator, Map<String, Object> options)
    {
        int size = getSize(options);

        // to always start on a letter, so we do not need to wrap in ''
        char firstChar = FIRST_CHARS[random.nextInt(6)];
        String uuid = UUID.randomUUID().toString();
        String uuidWithoutHyphens = PATTERN.matcher(uuid).replaceAll("");
        String name = firstChar + uuidWithoutHyphens.substring(1);

        name = name.substring(0, size);
        name = enrich(NAME_PREFIX_KEY, name, options);
        name = enrich(NAME_SUFFIX_KEY, name, options);

        return name;
    }

    @Nonnull
    @Override
    public CustomGuardrailConfig getParameters()
    {
        return config;
    }

    @Override
    public void validateParameters() throws ConfigurationException
    {
        if (minimumNameSize < DEFAULT_MINIMUM_NAME_SIZE || minimumNameSize > MAXIMUM_NAME_SIZE)
            throw new ConfigurationException(MINIMUM_NAME_SIZE_CONFIG_OPTION + " has to be at least " + DEFAULT_MINIMUM_NAME_SIZE + " and at most " + MAXIMUM_NAME_SIZE);
    }

    private String enrich(String key, String generatedValue, Map<String, Object> options)
    {
        if (options == null || options.isEmpty())
            return generatedValue;

        if (options.containsKey(key))
        {
            Object value = options.get(key);

            if (value == null)
                throw new IllegalArgumentException("Value of " + key + " cannot be null.");

            if (!(value instanceof String))
                throw new IllegalArgumentException("Value of " + key + " is not a string.");

            if (NAME_PREFIX_KEY.equals(key))
                generatedValue = value + generatedValue;
            else if (NAME_SUFFIX_KEY.equals(key))
                generatedValue = generatedValue + value;
        }

        return generatedValue;
    }

    private int getSize(Map<String, Object> options)
    {
        Object sizeObject;
        if (options == null)
        {
            sizeObject = MAXIMUM_NAME_SIZE;
        }
        else if (options.containsKey(NAME_SIZE))
        {
            Object nameSizeValue = options.get(NAME_SIZE);
            if (nameSizeValue != null)
                sizeObject = nameSizeValue;
            else
                throw new IllegalArgumentException("Value of " + NAME_SIZE + " has to be strictly positive integer.");
        }
        else
        {
            sizeObject = MAXIMUM_NAME_SIZE;
        }

        int size;

        if (sizeObject instanceof String)
        {
            try
            {
                size = Integer.parseInt((String) sizeObject);
            }
            catch (Throwable t)
            {
                throw new IllegalArgumentException("Value '" + sizeObject + "' can't be converted to integer.");
            }
        }
        else if (sizeObject instanceof Number)
            size = ((Number) sizeObject).intValue();
        else
            throw new IllegalArgumentException("Unsupported object passed to resolve " + NAME_SIZE + ": " + sizeObject.getClass().getName());

        if (size < minimumNameSize)
            throw new IllegalArgumentException("Value of " + NAME_SIZE + " parameter has to be at least " + minimumNameSize + '.');

        if (size > MAXIMUM_NAME_SIZE)
            throw new IllegalArgumentException("Generator generates names of maximum length " + MAXIMUM_NAME_SIZE + ". " +
                                               "You want to generate with length " + size + '.');

        return size;
    }
}
