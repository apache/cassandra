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
package org.apache.cassandra.auth;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Optional;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.FBUtilities;

public class RoleOptions
{
    private final Map<IRoleManager.Option, Object> options = new HashMap<>();

    /**
     * Set a value for a specific option.
     * Throws SyntaxException if the same option is set multiple times
     * @param option
     * @param value
     */
    public void setOption(IRoleManager.Option option, Object value)
    {
        if (options.containsKey(option))
            throw new SyntaxException(String.format("Multiple definition for property '%s'", option.name()));
        options.put(option, value);
    }

    /**
     * Return true if there are no options with values set, false otherwise
     * @return whether any options have values set or not
     */
    public boolean isEmpty()
    {
        return options.isEmpty();
    }

    /**
     * Return a map of all the options which have been set
     * @return all options with values
     */
    public Map<IRoleManager.Option, Object> getOptions()
    {
        return options;
    }

    /**
     * Return a boolean value of the superuser option
     * @return superuser option value
     */
    public Optional<Boolean> getSuperuser()
    {
        return Optional.fromNullable((Boolean)options.get(IRoleManager.Option.SUPERUSER));
    }

    /**
     * Return a boolean value of the login option
     * @return login option value
     */
    public Optional<Boolean> getLogin()
    {
        return Optional.fromNullable((Boolean)options.get(IRoleManager.Option.LOGIN));
    }

    /**
     * Return the string value of the password option
     * @return password option value
     */
    public Optional<String> getPassword()
    {
        return Optional.fromNullable((String)options.get(IRoleManager.Option.PASSWORD));
    }

    /**
     * Return a Map<String, String> representing custom options
     * It is the responsiblity of IRoleManager implementations which support
     * IRoleManager.Option.OPTION to handle type checking and conversion of these
     * values, if present
     * @return map of custom options
     */
    @SuppressWarnings("unchecked")
    public Optional<Map<String, String>> getCustomOptions()
    {
        return Optional.fromNullable((Map<String, String>)options.get(IRoleManager.Option.OPTIONS));
    }

    /**
     * Validate the contents of the options in two ways:
     * - Ensure that only a subset of the options supported by the configured IRoleManager are set
     * - Validate the type of any option values present.
     * Should either condition fail, then InvalidRequestException is thrown. This method is called
     * during validation of CQL statements, so the IRE results in a error response to the client.
     *
     * @throws InvalidRequestException if any options which are not supported by the configured IRoleManager
     *     are set or if any option value is of an incorrect type.
     */
    public void validate()
    {
        for (Map.Entry<IRoleManager.Option, Object> option : options.entrySet())
        {
            if (!DatabaseDescriptor.getRoleManager().supportedOptions().contains(option.getKey()))
                throw new InvalidRequestException(String.format("%s doesn't support %s",
                                                                DatabaseDescriptor.getRoleManager().getClass().getName(),
                                                                option.getKey()));
            switch (option.getKey())
            {
                case LOGIN:
                case SUPERUSER:
                    if (!(option.getValue() instanceof Boolean))
                        throw new InvalidRequestException(String.format("Invalid value for property '%s'. " +
                                                                        "It must be a boolean",
                                                                        option.getKey()));
                    break;
                case PASSWORD:
                    if (!(option.getValue() instanceof String))
                        throw new InvalidRequestException(String.format("Invalid value for property '%s'. " +
                                                                        "It must be a string",
                                                                        option.getKey()));
                    break;
                case OPTIONS:
                    if (!(option.getValue() instanceof Map))
                        throw new InvalidRequestException(String.format("Invalid value for property '%s'. " +
                                                                        "It must be a map",
                                                                        option.getKey()));
                    break;

            }
        }
    }

    public String toString()
    {
        return FBUtilities.toString(options);
    }
}
