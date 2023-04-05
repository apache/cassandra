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

package org.apache.cassandra.simulator.asm;

import java.util.function.Predicate;

import org.apache.cassandra.distributed.api.IClassTransformer;

// an adapter to IClassTransformer that is loaded by the system classloader
public class InterceptAsClassTransformer extends InterceptClasses implements IClassTransformer
{
    public InterceptAsClassTransformer(ChanceSupplier monitorDelayChance, ChanceSupplier nemesisChance, NemesisFieldKind.Selector nemesisFieldSelector, ClassLoader prewarmClassLoader, Predicate<String> prewarm)
    {
        super(monitorDelayChance, nemesisChance, nemesisFieldSelector, prewarmClassLoader, prewarm);
    }

    public InterceptAsClassTransformer(int api, ChanceSupplier monitorDelayChance, ChanceSupplier nemesisChance, NemesisFieldKind.Selector nemesisFieldSelector, ClassLoader prewarmClassLoader, Predicate<String> prewarm)
    {
        super(api, monitorDelayChance, nemesisChance, nemesisFieldSelector, prewarmClassLoader, prewarm);
    }

    @Override
    public byte[] transform(String name, byte[] bytecode)
    {
        return apply(name, bytecode);
    }

    @Override
    public IClassTransformer initialise()
    {
        return new SubTransformer()::apply;
    }
}
