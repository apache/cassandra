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
package org.apache.cassandra.cql3.functions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Objects;

import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.schema.SchemaConstants;

public final class FunctionName
{
    private static final Set<Character> DISALLOWED_CHARACTERS = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList('/', '[', ']')));

    // We special case the token function because that's the only function which name is a reserved keyword
    private static final FunctionName TOKEN_FUNCTION_NAME = FunctionName.nativeFunction("token");

    public final String keyspace;
    public final String name;

    public static FunctionName nativeFunction(String name)
    {
        return new FunctionName(SchemaConstants.SYSTEM_KEYSPACE_NAME, name);
    }

    /**
     * Validate the function name, e.g. contains no disallowed characters
     * @param name
     * @return true if name is valid; otherwise, false
     */
    public static boolean isNameValid(String name)
    {
        for (int i = 0; i < name.length(); i++)
        {
            char c = name.charAt(i);
            if (DISALLOWED_CHARACTERS.contains(c))
            {
                return false;
            }
        }
        return true;
    }

    public FunctionName(String keyspace, String name)
    {
        assert name != null : "Name parameter must not be null";
        this.keyspace = keyspace;
        this.name = name;
    }

    public FunctionName asNativeFunction()
    {
        return FunctionName.nativeFunction(name);
    }

    public boolean hasKeyspace()
    {
        return keyspace != null;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(keyspace, name);
    }

    @Override
    public final boolean equals(Object o)
    {
        if (!(o instanceof FunctionName))
            return false;

        FunctionName that = (FunctionName)o;
        return Objects.equal(this.keyspace, that.keyspace)
            && Objects.equal(this.name, that.name);
    }

    public final boolean equalsNativeFunction(FunctionName nativeFunction)
    {
        assert nativeFunction.keyspace.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME);
        if (this.hasKeyspace() && !this.keyspace.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME))
            return false;

        return Objects.equal(this.name, nativeFunction.name);
    }

    @Override
    public String toString()
    {
        return keyspace == null ? name : keyspace + "." + name;
    }

    public void appendCqlTo(CqlBuilder builder)
    {
        if (equalsNativeFunction(TOKEN_FUNCTION_NAME))
        {
            builder.append(name);
        }
        else
        {
            if (keyspace != null)
            {
                builder.appendQuotingIfNeeded(keyspace)
                       .append('.');
            }
            builder.appendQuotingIfNeeded(name);
        }
    }
}
