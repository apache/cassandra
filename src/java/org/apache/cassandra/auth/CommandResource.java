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
package org.apache.cassandra.auth;

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.Sets;

/**
 * Resource representing a management command invoked via CQL ({@code INVOKE COMMAND}).
 * <p>
 * This is a placeholder: there is no CQL grammar to {@code GRANT}/{@code REVOKE}
 * permissions on it yet, so no role can currently be granted {@link Permission#EXECUTE} here.
 */
public class CommandResource implements IResource
{
    enum Level
    {
        ROOT, COMMAND
    }

    private static final String ROOT_NAME = "command";
    private static final CommandResource ROOT_RESOURCE = new CommandResource();
    private static final Set<Permission> COMMAND_PERMISSIONS = Sets.immutableEnumSet(Permission.EXECUTE);

    private final Level level;
    private final String name;

    private CommandResource()
    {
        level = Level.ROOT;
        name = null;
    }

    private CommandResource(String name)
    {
        this.name = name;
        level = Level.COMMAND;
    }

    public static CommandResource root()
    {
        return ROOT_RESOURCE;
    }

    public static CommandResource command(String commandName)
    {
        return new CommandResource(commandName);
    }

    @Override
    public String getName()
    {
        return level == Level.ROOT ? ROOT_NAME : String.format("%s/%s", ROOT_NAME, name);
    }

    @Override
    public IResource getParent()
    {
        if (level == Level.COMMAND)
            return root();
        throw new IllegalStateException("Root-level resource can't have a parent");
    }

    @Override
    public boolean hasParent()
    {
        return level != Level.ROOT;
    }

    @Override
    public boolean exists()
    {
        return true;
    }

    @Override
    public Set<Permission> applicablePermissions()
    {
        return COMMAND_PERMISSIONS;
    }

    @Override
    public String toString()
    {
        return level == Level.ROOT ? "<all commands>" : String.format("<command %s>", name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof CommandResource))
            return false;

        CommandResource c = (CommandResource) o;
        return level == c.level && Objects.equal(name, c.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(level, name);
    }
}
