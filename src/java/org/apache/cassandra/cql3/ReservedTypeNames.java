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

import java.util.Set;

import com.google.common.collect.Sets;

/**
 * Set of reserved type names. When creating a user type via CQL, if the type's name is included in this
 * set, the statement will fail unless the name is double quoted.
 */
public class ReservedTypeNames
{
    public static final Set<String> reservedTypeNames = Sets.newHashSet(
    "byte",
    "complex",
    "enum",
    "date",
    "interval",
    "macaddr",
    "bitstring");

    public static boolean isReserved(String typeName)
    {
        return reservedTypeNames.contains(typeName);
    }
}