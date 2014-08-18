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

import com.google.common.base.Objects;

public class FunctionName
{
    public final String namespace;
    public final String name;

    // Use by toString rather than built from 'bundle' and 'name' so as to
    // preserve the original case.
    private final String displayName;

    public FunctionName(String name)
    {
        this("", name);
    }

    public FunctionName(String namespace, String name)
    {
        this.namespace = namespace.toLowerCase();
        this.name = name.toLowerCase();

        this.displayName = namespace.isEmpty() ? name : namespace + "::" + name;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hashCode(namespace, name);
    }

    @Override
    public final boolean equals(Object o)
    {
        if (!(o instanceof FunctionName))
            return false;

        FunctionName that = (FunctionName)o;
        return Objects.equal(this.namespace, that.namespace)
            && Objects.equal(this.name, that.name);
    }

    @Override
    public String toString()
    {
        return displayName;
    }
}
