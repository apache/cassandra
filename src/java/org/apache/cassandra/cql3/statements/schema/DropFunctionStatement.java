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
package org.apache.cassandra.cql3.statements.schema;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.functions.FunctionName;
import org.apache.cassandra.cql3.functions.UDFunction;
import org.apache.cassandra.cql3.functions.UserFunction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;

import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import static com.google.common.collect.Iterables.transform;

public final class DropFunctionStatement extends AlterSchemaStatement
{
    private final String functionName;
    private final Collection<CQL3Type.Raw> arguments;
    private final boolean argumentsSpeficied;
    private final boolean ifExists;

    public DropFunctionStatement(String keyspaceName,
                                 String functionName,
                                 Collection<CQL3Type.Raw> arguments,
                                 boolean argumentsSpeficied,
                                 boolean ifExists)
    {
        super(keyspaceName);
        this.functionName = functionName;
        this.arguments = arguments;
        this.argumentsSpeficied = argumentsSpeficied;
        this.ifExists = ifExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        String name =
            argumentsSpeficied
          ? format("%s.%s(%s)", keyspaceName, functionName, join(", ", transform(arguments, CQL3Type.Raw::toString)))
          : format("%s.%s", keyspaceName, functionName);

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
        {
            if (ifExists)
                return schema;

            throw ire("Function '%s' doesn't exist", name);
        }

        Collection<UserFunction> functions = keyspace.userFunctions.get(new FunctionName(keyspaceName, functionName));
        if (functions.size() > 1 && !argumentsSpeficied)
        {
            throw ire("'DROP FUNCTION %s' matches multiple function definitions; " +
                      "specify the argument types by issuing a statement like " +
                      "'DROP FUNCTION %s (type, type, ...)'. You can use cqlsh " +
                      "'DESCRIBE FUNCTION %s' command to find all overloads",
                      functionName, functionName, functionName);
        }

        arguments.stream()
                 .filter(raw -> !raw.isImplicitlyFrozen() && raw.isFrozen())
                 .findFirst()
                 .ifPresent(t -> { throw ire("Argument '%s' cannot be frozen; remove frozen<> modifier from '%s'", t, t); });

        List<AbstractType<?>> argumentTypes = prepareArgumentTypes(keyspace.types);

        Predicate<UserFunction> filter = UserFunctions.Filter.UDF;
        if (argumentsSpeficied)
            filter = filter.and(f -> f.typesMatch(argumentTypes));

        UserFunction function = functions.stream().filter(filter).findAny().orElse(null);
        if (null == function)
        {
            if (ifExists)
                return schema;

            throw ire("Function '%s' doesn't exist", name);
        }

        String dependentAggregates =
            keyspace.userFunctions
                    .aggregatesUsingFunction(function)
                    .map(a -> a.name().toString())
                    .collect(joining(", "));

        if (!dependentAggregates.isEmpty())
            throw ire("Function '%s' is still referenced by aggregates %s", name, dependentAggregates);

        String dependentTables = keyspace.tablesUsingFunction(function)
                                         .map(table -> table.name)
                                         .collect(joining(", "));

        if (!dependentTables.isEmpty())
            throw ire("Function '%s' is still referenced by column masks in tables %s", name, dependentTables);

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.userFunctions.without(function)));
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        UserFunctions dropped = diff.altered.get(0).udfs.dropped;
        assert dropped.size() == 1;
        return SchemaChange.forFunction(Change.DROPPED, (UDFunction) dropped.iterator().next());
    }

    public void authorize(ClientState client)
    {
        KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
        if (null == keyspace)
            return;

        Stream<UserFunction> functions = keyspace.userFunctions.get(new FunctionName(keyspaceName, functionName)).stream();
        if (argumentsSpeficied)
            functions = functions.filter(f -> f.typesMatch(prepareArgumentTypes(keyspace.types)));

        functions.forEach(f -> client.ensurePermission(Permission.DROP, FunctionResource.function(f)));
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_FUNCTION, keyspaceName, functionName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, functionName);
    }

    private List<AbstractType<?>> prepareArgumentTypes(Types types)
    {
        return arguments.stream()
                        .map(t -> t.prepare(keyspaceName, types))
                        .map(t -> t.getType().udfType())
                        .collect(toList());
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final FunctionName name;
        private final List<CQL3Type.Raw> arguments;
        private final boolean argumentsSpecified;
        private final boolean ifExists;

        public Raw(FunctionName name,
                   List<CQL3Type.Raw> arguments,
                   boolean argumentsSpecified,
                   boolean ifExists)
        {
            this.name = name;
            this.arguments = arguments;
            this.argumentsSpecified = argumentsSpecified;
            this.ifExists = ifExists;
        }

        public DropFunctionStatement prepare(ClientState state)
        {
            String keyspaceName = name.hasKeyspace() ? name.keyspace : state.getKeyspace();
            return new DropFunctionStatement(keyspaceName, name.name, arguments, argumentsSpecified, ifExists);
        }
    }
}
