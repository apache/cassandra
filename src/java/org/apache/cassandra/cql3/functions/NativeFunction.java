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

import javax.annotation.Nullable;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.transport.ProtocolVersion;

/**
 * Base class for our native/hardcoded functions.
 */
public abstract class NativeFunction extends AbstractFunction
{
    protected NativeFunction(String name, AbstractType<?> returnType, AbstractType<?>... argTypes)
    {
        super(FunctionName.nativeFunction(name), Arrays.asList(argTypes), returnType);
    }

    @Override
    public final boolean isNative()
    {
        return true;
    }

    @Override
    public boolean isPure()
    {
        // Most of our functions are pure, the other ones should override this
        return true;
    }

    /**
     * Returns a copy of this function using its old pre-5.0 name before the adoption of snake-cased function names.
     * Those naming conventions were adopted in 5.0, but we still need to support the old names for
     * compatibility. See CASSANDRA-18037 for further details.
     *
     * @return a copy of this function using its old pre-5.0 deprecated name, or {@code null} if the pre-5.0 function
     * name already satisfied the naming conventions.
     */
    @Nullable
    public NativeFunction withLegacyName()
    {
        return null;
    }

    @Override
    public Arguments newArguments(ProtocolVersion version)
    {
        return FunctionArguments.newInstanceForNativeFunction(version, argTypes);
    }

}
