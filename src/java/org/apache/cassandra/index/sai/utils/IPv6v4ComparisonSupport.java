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
package org.apache.cassandra.index.sai.utils;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Index config property for defining the behaviour of the comparison between IPv4 and IPv6 addresses when the index is used.
 * </p>
 * The inet type in CQL allows ipv4 and ipv6 address formats to mix in the same column, and while there is a standard
 * conversion between v4 and v6, equivalent addresses are not considered equal by Cassandra, when it comes to their usage
 * in keys or in filtering queries. However, in SAI queries, equivalent v4 and v6 addresses ARE considered equal. While
 * it can be considered a bug that SAI does not respect the inet type's semantics, it is also a feature that can be useful.
 * However, for backwards compatibility reasons, we should allow users to let SAI queries behave same as filtering queries
 * through this index config property.
 */
public class IPv6v4ComparisonSupport
{
    public static final String IP_COMPARISON_OPTION = "compare_v4_to_v6_as_equal";
    public static final boolean IP_COMPARISON_OPTION_DEFAULT = false;

    @VisibleForTesting
    public static final String NOT_IP_ERROR = "The '" + IP_COMPARISON_OPTION + "' index option can only be used with the ip type.";

    @VisibleForTesting
    static final String WRONG_OPTION_ERROR = "Illegal value for boolean option '" +
            IPv6v4ComparisonSupport.IP_COMPARISON_OPTION + "': ";

    public static void fromMap(Map<String, String> options, AbstractType<?> type)
    {
        if (!options.containsKey(IP_COMPARISON_OPTION))
            return;

        if (!(type instanceof InetAddressType))
            throw new InvalidRequestException(NOT_IP_ERROR);

        String value = options.get(IP_COMPARISON_OPTION).toUpperCase();

        if (Strings.isNullOrEmpty(value))
            throw new InvalidRequestException(WRONG_OPTION_ERROR + value);

        validateBoolean(value);
    }

    private static void validateBoolean(String value)
    {
        if (!value.equalsIgnoreCase(Boolean.TRUE.toString()) && !value.equalsIgnoreCase(Boolean.FALSE.toString()))
            throw new InvalidRequestException(WRONG_OPTION_ERROR + value);
    }
}