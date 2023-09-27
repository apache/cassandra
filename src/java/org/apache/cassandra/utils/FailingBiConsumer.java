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

package org.apache.cassandra.utils;

import java.util.function.BiConsumer;

public interface FailingBiConsumer<A, B> extends BiConsumer<A, B>
{
    void acceptOrFail(A a, B b) throws Throwable;

    @Override
    default void accept(A a, B b)
    {
        try
        {
            acceptOrFail(a, b);
        }
        catch (Throwable e)
        {
            throw Throwables.throwAsUncheckedException(e);
        }
    }

    static <A, B> FailingBiConsumer<A, B> orFail(FailingBiConsumer<A, B> fn)
    {
        return fn;
    }
}
