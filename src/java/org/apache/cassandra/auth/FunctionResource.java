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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * IResource implementation representing functions.
 *
 * The root level "functions" resource represents the collection of all Functions.
 * "functions"                          - root level resource representing all functions defined across every keyspace
 * "functions/keyspace"                 - keyspace level resource to apply permissions to all functions within a keyspace
 * "functions/keyspace/function"        - a specific function, scoped to a given keyspace
 */
public class FunctionResource implements IResource
{
    enum Level
    {
        ROOT, KEYSPACE, FUNCTION
    }

    // permissions which may be granted on either a resource representing some collection of functions
    // i.e. the root resource (all functions) or a keyspace level resource (all functions in a given keyspace)
    private static final Set<Permission> COLLECTION_LEVEL_PERMISSIONS = Sets.immutableEnumSet(Permission.CREATE,
                                                                                              Permission.ALTER,
                                                                                              Permission.DROP,
                                                                                              Permission.AUTHORIZE,
                                                                                              Permission.EXECUTE);
    // permissions which may be granted on resources representing a specific function
    private static final Set<Permission> SCALAR_FUNCTION_PERMISSIONS = Sets.immutableEnumSet(Permission.ALTER,
                                                                                             Permission.DROP,
                                                                                             Permission.AUTHORIZE,
                                                                                             Permission.EXECUTE);

    private static final Set<Permission> AGGREGATE_FUNCTION_PERMISSIONS = Sets.immutableEnumSet(Permission.ALTER,
                                                                                                Permission.DROP,
                                                                                                Permission.AUTHORIZE,
                                                                                                Permission.EXECUTE);

    private static final String ROOT_NAME = "functions";
    private static final FunctionResource ROOT_RESOURCE = new FunctionResource();

    private final Level level;
    private final String keyspace;
    private final String name;
    private final List<AbstractType<?>> argTypes;

    private FunctionResource()
    {
        level = Level.ROOT;
        keyspace = null;
        name = null;
        argTypes = null;
    }

    private FunctionResource(String keyspace)
    {
        level = Level.KEYSPACE;
        this.keyspace = keyspace;
        name = null;
        argTypes = null;
    }

    private FunctionResource(String keyspace, String name, List<AbstractType<?>> argTypes)
    {
        level = Level.FUNCTION;
        this.keyspace = keyspace;
        this.name = name;
        this.argTypes = argTypes;
    }

    /**
     * @return the root-level resource.
     */
    public static FunctionResource root()
    {
        return ROOT_RESOURCE;
    }

    /**
     * Creates a FunctionResource representing the collection of functions scoped
     * to a specific keyspace.
     *
     * @param keyspace name of the keyspace
     * @return FunctionResource instance representing all of the keyspace's functions
     */
    public static FunctionResource keyspace(String keyspace)
    {
        return new FunctionResource(keyspace);
    }

    /**
     * Creates a FunctionResource representing a specific, keyspace-scoped function.
     *
     * @param keyspace the keyspace in which the function is scoped
     * @param name     name of the function.
     * @param argTypes the types of the arguments to the function
     * @return FunctionResource instance reresenting the function.
     */
    public static FunctionResource function(String keyspace, String name, List<AbstractType<?>> argTypes)
    {
        return new FunctionResource(keyspace, name, argTypes);
    }

    /**
     * Creates a FunctionResource representing a specific, keyspace-scoped function.
     * This variant is used to create an instance during parsing of a CQL statement.
     * It includes transposition of the arg types from CQL types to AbstractType
     * implementations
     *
     * @param keyspace the keyspace in which the function is scoped
     * @param name     name of the function.
     * @param argTypes the types of the function arguments in raw CQL form
     * @return FunctionResource instance reresenting the function.
     */
    public static FunctionResource functionFromCql(String keyspace, String name, List<CQL3Type.Raw> argTypes)
    {
        if (keyspace == null)
            throw new InvalidRequestException("In this context function name must be " +
                                              "explictly qualified by a keyspace");
        List<AbstractType<?>> abstractTypes = new ArrayList<>();
        for (CQL3Type.Raw cqlType : argTypes)
            abstractTypes.add(cqlType.prepare(keyspace).getType());

        return new FunctionResource(keyspace, name, abstractTypes);
    }

    /**
     * Parses a resource name into a FunctionResource instance.
     *
     * @param name Name of the function resource.
     * @return FunctionResource instance matching the name.
     */
    public static FunctionResource fromName(String name)
    {
        String[] parts = StringUtils.split(name, '/');

        if (!parts[0].equals(ROOT_NAME) || parts.length > 3)
            throw new IllegalArgumentException(String.format("%s is not a valid function resource name", name));

        if (parts.length == 1)
            return root();

        if (parts.length == 2)
            return keyspace(parts[1]);

        String[] nameAndArgs = StringUtils.split(parts[2], "[|]");
        return function(parts[1], nameAndArgs[0], argsListFromString(nameAndArgs[1]));
    }

    /**
     * @return Printable name of the resource.
     */
    public String getName()
    {
        switch (level)
        {
            case ROOT:
                return ROOT_NAME;
            case KEYSPACE:
                return String.format("%s/%s", ROOT_NAME, keyspace);
            case FUNCTION:
                return String.format("%s/%s/%s[%s]", ROOT_NAME, keyspace, name, argListAsString());
        }
        throw new AssertionError();
    }

    /**
     * Get the name of the keyspace this resource relates to. In the case of the
     * global root resource, return null
     *
     * @return the keyspace name of this resource, or null for the root resource
     */
    public String getKeyspace()
    {
        return keyspace;
    }

    /**
     * @return a qualified FunctionName instance for a function-level resource.
     * Throws IllegalStateException if called on the resource which doens't represent a single function.
     */
    public FunctionName getFunctionName()
    {
        if (level != Level.FUNCTION)
            throw new IllegalStateException(String.format("%s function resource has no function name", level));
        return new FunctionName(keyspace, name);
    }

    /**
     * @return Parent of the resource, if any. Throws IllegalStateException if it's the root-level resource.
     */
    public IResource getParent()
    {
        switch (level)
        {
            case KEYSPACE:
                return root();
            case FUNCTION:
                return keyspace(keyspace);
        }
        throw new IllegalStateException("Root-level resource can't have a parent");
    }

    public boolean hasParent()
    {
        return level != Level.ROOT;
    }

    public boolean exists()
    {
        switch (level)
        {
            case ROOT:
                return true;
            case KEYSPACE:
                return Schema.instance.getKeyspaces().contains(keyspace);
            case FUNCTION:
                return Schema.instance.findFunction(getFunctionName(), argTypes).isPresent();
        }
        throw new AssertionError();
    }

    public Set<Permission> applicablePermissions()
    {
        switch (level)
        {
            case ROOT:
            case KEYSPACE:
                return COLLECTION_LEVEL_PERMISSIONS;
            case FUNCTION:
            {
                Optional<Function> function = Schema.instance.findFunction(getFunctionName(), argTypes);
                assert function.isPresent() : "Unable to find function object for resource " + toString();
                return function.get().isAggregate() ? AGGREGATE_FUNCTION_PERMISSIONS : SCALAR_FUNCTION_PERMISSIONS;
            }
        }
        throw new AssertionError();
    }

    public int compareTo(FunctionResource o)
    {
        return this.name.compareTo(o.name);
    }

    @Override
    public String toString()
    {
        switch (level)
        {
            case ROOT:
                return "<all functions>";
            case KEYSPACE:
                return String.format("<all functions in %s>", keyspace);
            case FUNCTION:
                return String.format("<function %s.%s(%s)>",
                                     keyspace,
                                     name,
                                     Joiner.on(", ").join(AbstractType.asCQLTypeStringList(argTypes)));
        }
        throw new AssertionError();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof FunctionResource))
            return false;

        FunctionResource f = (FunctionResource) o;

        return Objects.equal(level, f.level)
               && Objects.equal(keyspace, f.keyspace)
               && Objects.equal(name, f.name)
               && Objects.equal(argTypes, f.argTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(level, keyspace, name, argTypes);
    }

    private String argListAsString()
    {
        return Joiner.on("^").join(argTypes);
    }

    private static List<AbstractType<?>> argsListFromString(String s)
    {
        List<AbstractType<?>> argTypes = new ArrayList<>();
        for(String type : Splitter.on("^").omitEmptyStrings().trimResults().split(s))
            argTypes.add(TypeParser.parse(type));
        return argTypes;
    }
}
