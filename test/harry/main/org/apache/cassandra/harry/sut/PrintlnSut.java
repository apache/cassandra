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

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.cassandra.harry.core.Configuration;

public class PrintlnSut implements SystemUnderTest
{
    public boolean isShutdown()
    {
        return false;
    }

    public void shutdown()
    {
    }

    public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
    {
        System.out.println(String.format("%s | %s | %s",
                                         statement,
                                         cl,
                                         Arrays.toString(bindings)));
        return new Object[0][];
    }

    public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
    {
        return CompletableFuture.supplyAsync(() -> execute(statement, cl, bindings),
                                             Runnable::run);
    }

    @JsonTypeName("println")
    public static class PrintlnSutConfiguration implements Configuration.SutConfiguration
    {
        @JsonCreator
        public PrintlnSutConfiguration()
        {

        }
        public SystemUnderTest make()
        {
            return new PrintlnSut();
        }
    }
}
