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

package org.apache.cassandra.management;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandMetadata;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CommandMBeanAdapterTest
{
    private static CommandMBeanAdapter newAdapter()
    {
        @SuppressWarnings("unchecked")
        Command<Void> command = Mockito.mock(Command.class);
        Mockito.when(command.metadata()).thenReturn(Mockito.mock(CommandMetadata.class));
        // Executor must never be reached for the invalid-argument cases below.
        CommandInvokerService.Executor executor = (name, args) -> {
            throw new AssertionError("Executor should not be invoked for invalid arguments");
        };
        return new CommandMBeanAdapter("version", command, executor);
    }

    @Test
    public void invokeRejectsNonStringArgument()
    {
        assertThatThrownBy(() -> newAdapter().invoke("invoke", new Object[]{ 42 }, new String[]{ "int" }))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly one parameter");
    }

    @Test
    public void invokeRejectsWrongArgumentCount()
    {
        assertThatThrownBy(() -> newAdapter().invoke("invoke", new Object[]{ "a", "b" }, new String[]{ "String", "String" }))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly one parameter");
    }

    @Test
    public void invokeRejectsNullParams()
    {
        assertThatThrownBy(() -> newAdapter().invoke("invoke", null, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly one parameter");
    }

    @Test
    public void invokeRejectsUnknownAction()
    {
        assertThatThrownBy(() -> newAdapter().invoke("bogus", new Object[]{ "{}" }, new String[]{ "String" }))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Unknown operation");
    }
}
