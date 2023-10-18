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

package org.apache.cassandra.schema;

import java.util.Objects;
import java.util.UUID;

import com.google.common.base.Preconditions;

/**
 * Immutable snapshot of the current schema along with its version.
 */
public class DistributedSchema
{
    public static final DistributedSchema EMPTY = new DistributedSchema(Keyspaces.none(), SchemaConstants.emptyVersion);

    private final Keyspaces keyspaces;
    private final UUID version;

    public DistributedSchema(Keyspaces keyspaces, UUID version)
    {
        Objects.requireNonNull(keyspaces);
        Objects.requireNonNull(version);
        this.keyspaces = keyspaces;
        this.version = version;
        validate();
    }

    public Keyspaces getKeyspaces()
    {
        return keyspaces;
    }

    public boolean isEmpty()
    {
        return SchemaConstants.emptyVersion.equals(version);
    }

    public UUID getVersion()
    {
        return version;
    }

    /**
     * Converts the given schema version to a string. Returns {@code unknown}, if {@code version} is {@code null}
     * or {@code "(empty)"}, if {@code version} refers to an {@link SchemaConstants#emptyVersion} schema.
     */
    public static String schemaVersionToString(UUID version)
    {
        return version == null
               ? "unknown"
               : SchemaConstants.emptyVersion.equals(version)
                 ? "(empty)"
                 : version.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DistributedSchema schema = (DistributedSchema) o;
        return keyspaces.equals(schema.keyspaces) && version.equals(schema.version);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspaces, version);
    }

    private void validate()
    {
        keyspaces.forEach(ksm -> {
            ksm.tables.forEach(tm -> Preconditions.checkArgument(tm.keyspace.equals(ksm.name), "Table %s metadata points to keyspace %s while defined in keyspace %s", tm.name, tm.keyspace, ksm.name));
            ksm.views.forEach(vm -> Preconditions.checkArgument(vm.keyspace().equals(ksm.name), "View %s metadata points to keyspace %s while defined in keyspace %s", vm.name(), vm.keyspace(), ksm.name));
            ksm.types.forEach(ut -> Preconditions.checkArgument(ut.keyspace.equals(ksm.name), "Type %s points to keyspace %s while defined in keyspace %s", ut.name, ut.keyspace, ksm.name));
            ksm.userFunctions.forEach(f -> Preconditions.checkArgument(f.name().keyspace.equals(ksm.name), "Function %s points to keyspace %s while defined in keyspace %s", f.name().name, f.name().keyspace, ksm.name));
        });
    }
}
