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
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;

public abstract class AbstractFunction implements Function
{
    public final String name;
    public final List<AbstractType<?>> argsType;
    public final AbstractType<?> returnType;

    protected AbstractFunction(String name, AbstractType<?> returnType, AbstractType<?>... argsType)
    {
        this.name = name;
        this.argsType = Arrays.asList(argsType);
        this.returnType = returnType;
    }

    public String name()
    {
        return name;
    }

    public List<AbstractType<?>> argsType()
    {
        return argsType;
    }

    public AbstractType<?> returnType()
    {
        return returnType;
    }

    // Most of our functions are pure, the other ones should override this
    public boolean isPure()
    {
        return true;
    }

    /**
     * Creates a trivial factory that always return the provided function.
     */
    public static Function.Factory factory(final Function fun)
    {
        return new Function.Factory()
        {
            public Function create(String ksName, String cfName)
            {
                return fun;
            }
        };
    }
}
