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

package org.apache.cassandra.tools.nodetool;

import javax.inject.Inject;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;

import picocli.CommandLine;
import picocli.CommandLine.ExecutionException;

/**
 * Abstract class for all nodetool commands, which provides common methods and fields
 * for running commands and outputting results.
 * <p>
 * The command is executed by calling {@link #execute(NodeProbe)}, in all other cases
 * it should not contain any fields or methods that are specific to a particular API
 * being executed, or common methods that are shared across multiple commands.
 * <p>
 * Commands must work only with the API provided by the {@link NodeProbe} instance.
 */
public abstract class AbstractCommand implements Runnable
{
    @Inject
    protected Output output;

    private NodeProbe probe;

    public void probe(NodeProbe probe)
    {
        this.probe = probe;
    }

    public NodeProbe probe()
    {
        return probe;
    }

    public void logger(Output output)
    {
        this.output = output;
    }

    @Override
    public void run()
    {
        execute(probe());
    }

    /**
     * Prepare a command for execution. This method is called before the command is executed and
     * can be used to perform any necessary setup or validation. If this method returns {@code false},
     * the command will not initiate connection and will be executed locally. The default implementation
     * returns {@code true} so that the command initiates connection to the node before execution.
     *
     * @return {@code true} if the command is required to connect to the node, {@code false} otherwise.
     * @throws ExecutionException if an error occurs during preparation and execution must be aborted.
     */
    protected boolean shouldConnect() throws ExecutionException
    {
        return true;
    }

    /**
     * Execute the command using the supplied {@link NodeProbe} instance, which is already connected
     * to the node and ready to use. If the {@link NodeProbe} is not {@code null}, it is guaranteed that it
     * is connected to the node, and the command can use it to perform operations on the node.
     * <p>
     * WARNING:
     * <p>
     * Due to the backwards compatibility with the previous Airline-based implementation of
     * the nodetool commands, for most of the commands this method is also used to validate the input
     * arguments and perform necessary checks before the command is executed. This implies that
     * the command <u>throws an exception during execution</u> to avoid unexpected behavior or errors,
     * instead of validating the input arguments within the Picocli framework on the Parser stage.
     * For this reason, the {@link CommandLine.Parameters#arity()} and {@link CommandLine.Option#arity()}
     * which are normally used to validate the input arguments, set to {@code "0..*"} or {@code "0..1"}
     * making the arguments optional, and passing the validation to the command's
     * {@code execute(NodeProbe probe)} method.
     * <p>
     * New commands should not rely on the behavior described above and should validate the input arguments
     * using Picocli annotations such as {@link CommandLine.Parameters} and {@link CommandLine.Option}.
     *
     * @param probe The {@link NodeProbe} instance to use, or {@code null} if no connection is required.
     */
    protected abstract void execute(NodeProbe probe);
}
