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

package org.apache.cassandra.management.picocli;

import java.lang.reflect.Field;

import javax.inject.Inject;

import org.apache.cassandra.management.api.Command;
import org.apache.cassandra.management.api.CommandExecutionArgs;
import org.apache.cassandra.management.api.CommandExecutionContext;
import org.apache.cassandra.management.api.CommandMetadata;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.Output;
import org.apache.cassandra.tools.nodetool.AbstractCommand;

import picocli.CommandLine;

public class PicocliCommandAdapter implements Command<Void>
{
    private final Class<? extends AbstractCommand> commandClass;
    private final CommandMetadata commandMetadata;

    public PicocliCommandAdapter(Class<? extends AbstractCommand> commandClass)
    {
        this.commandClass = commandClass;
        this.commandMetadata = PicocliCommandMetadata.from(commandClass);
        if (!(commandMetadata instanceof PicocliCommandMetadata))
            throw new IllegalStateException("CommandMetadata must be PicocliCommandMetadata for picocli commands");
    }

    @Override
    public CommandMetadata metadata()
    {
        return commandMetadata;
    }

    @Override
    public Void execute(CommandExecutionArgs arguments, CommandExecutionContext context)
    {
        Output output = context.service(Output.class);
        CommandLine commandLine = new CommandLine(commandClass, new InjectCassandraContext(output));
        Object userCommand = commandLine.getCommand();

        if (userCommand instanceof AbstractCommand)
            ((AbstractCommand) userCommand).probe(context.service(NodeProbe.class));

        PicocliCommandArgsConverter.toCommand(arguments, userCommand);

        // Alternatively, we could convert CommandExecutionArgs back to String[] and let picocli parse and
        // execute; however, Cassandra validates a lot of arguments inside the command's execution body
        // (historically because of Airline limitations), so that would parse and validate everything twice.
        // Hence picocli's argument validation and error handling are not used here.
        if (userCommand instanceof Runnable)
        {
            ((Runnable) userCommand).run();
            return null;
        }
        else
        {
            throw new RuntimeException(String.format("Unsupported command type: %s. " +
                                                     "Command class must implement Runnable to be executed.",
                                                     userCommand.getClass().getName()));
        }
    }

    private static class InjectCassandraContext implements CommandLine.IFactory
    {
        private final Output output;
        private final CommandLine.IFactory fallback;

        public InjectCassandraContext(Output output)
        {
            this.fallback = CommandLine.defaultFactory();
            this.output = output;
        }

        @Override
        public <K> K create(Class<K> cls)
        {
            try
            {
                K bean = this.fallback.create(cls);
                Class<?> beanClass = bean.getClass();
                do
                {
                    Field[] fields = beanClass.getDeclaredFields();
                    for (Field field : fields)
                    {
                        if (!field.isAnnotationPresent(Inject.class))
                            continue;
                        
                        field.setAccessible(true);
                        if (field.getType().equals(Output.class))
                        {
                            field.set(bean, output);
                        }
                        else
                        {
                            throw new RuntimeException("Unsupported injectable field type: " + field.getType() +
                                                       " in class " + beanClass.getName() + ". " +
                                                       "Only Output is supported for injection.");
                        }
                    }
                }
                while ((beanClass = beanClass.getSuperclass()) != null);
                return bean;
            }
            catch (Exception e)
            {
                throw new CommandLine.InitializationException("Failed to create instance of " + cls, e);
            }
        }
    }
}
