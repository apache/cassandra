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

package org.apache.cassandra.cql3.functions.masking;

import org.apache.cassandra.cql3.functions.NativeFunctions;

/**
 * A collection of {@link MaskingFunction}s for dynamic data masking, meant to obscure the real value of a column.
 */
public class MaskingFcts
{
    /** Adds all the available native data masking functions to the specified native functions. */
    public static void addFunctionsTo(NativeFunctions functions)
    {
        functions.add(NullMaskingFunction.factory());
        functions.add(ReplaceMaskingFunction.factory());
        functions.add(DefaultMaskingFunction.factory());
        functions.add(HashMaskingFunction.factory());
        PartialMaskingFunction.factories().forEach(functions::add);
    }
}
