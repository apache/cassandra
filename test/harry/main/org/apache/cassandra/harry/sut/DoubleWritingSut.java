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

package org.apache.cassandra.harry.sut;

import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;

public class DoubleWritingSut implements SystemUnderTest
{
    private final SystemUnderTest primary;
    private final SystemUnderTest secondary;

    public DoubleWritingSut(SystemUnderTest primary,
                            SystemUnderTest secondary)
    {
        this.primary = primary;
        this.secondary = secondary;
    }
    public boolean isShutdown()
    {
        return primary.isShutdown();
    }

    public void shutdown()
    {
        primary.shutdown();
    }

    private static final Pattern pattern = Pattern.compile("^select", Pattern.CASE_INSENSITIVE);

    public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
    {
        if (pattern.matcher(statement).find())
            return primary.execute(statement, cl, bindings);

        secondary.execute(statement, cl, bindings);
        return primary.execute(statement, cl, bindings);
    }

    public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
    {
        throw new UnsupportedOperationException("Not implemented (yet)");
    }
}
