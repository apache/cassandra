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

import java.io.IOException;
import java.util.Optional;

import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;

public interface SchemaTransformation
{
    SchemaTransformationSerializer serializer = new SchemaTransformationSerializer();

    /**
     * Apply a statement transformation to a schema snapshot.
     * <p>
     * Implementing methods should be side-effect free (outside of throwing exceptions if the transformation cannot
     * be successfully applied to the provided schema).
     *
     * @param metadata Cluster metadata representing the current state, including the DistributedSchema with the
     *                 Keyspaces to base the transformation on
     * @return Keyspaces transformed by the statement
     */
    Keyspaces apply(ClusterMetadata metadata);

    default String cql()
    {
        return "null";
    }

    /**
     * SchemaTransformation::apply should be side effect free, as it may be called repeatedly for any given
     * transformation.
     * <ul>
     *   <li>On receiving a CQL DDL statement, the coordinator pre-emptively applies it using its own current cluster
     *   metadata for validation. The result of this transformation is discarded and the coordinator submits to the CMS.
     *   </li>
     *   <li>A CMS member, receiving a Commit containing an AlterSchema, applies the enclosed SchemaTransformation before
     *   committing to the metadata log. There may be multiple attempts to commit, and so multiple applications, if
     *   there is contention on the metadata log.
     *   </li>
     *   <li>Following commit, all peers should receive the committed log entry and they will then execute the AlterSchema,
     *   applying the SchemaTransformation locally. The result of this application then becomes the current published
     *   cluster metadata for the peer.
     *   </li>
     * </ul>
     * At the moment, not all SchemaTransformation are entirely free of side effects, because both client warnings and
     * guardrails are tightly coupled to the ST implementations and they do produce side effects. Ideally, ClientWarn
     * and Guardrails can be reworked to decouple them from ClientState and threadlocals. In the meantime, we can hint
     * to them that SchemaTransformation::apply is being called in the context of AlterSchema::execute and so they
     * should not produce their side effects. The default impl is a no-op, but AlterSchemaStatement which has access to
     * a ClientState, is able to pause both warnings and guardrails.
     * @see org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement#enterExecution()
     */
    default void enterExecution()
    {
    }

    default void exitExecution()
    {
    }

    default String keyspace()
    {
        return null;
    }

    /**
     * If the transformation should be applied with a certain timestamp, this method should be overriden. This is used
     * by {@link SchemaTransformations#updateSystemKeyspace(KeyspaceMetadata, long)} when we need to set the fixed
     * timestamp in order to preserve user settings.
     */
    default Optional<Long> fixedTimestampMicros()
    {
        return Optional.empty();
    }

    /**
     * The result of applying (on this node) a given schema transformation.
     */
    class SchemaTransformationResult
    {
        private final DistributedSchema before;
        private final DistributedSchema after;
        public final Keyspaces.KeyspacesDiff diff;

        public SchemaTransformationResult(DistributedSchema before, DistributedSchema after, Keyspaces.KeyspacesDiff diff)
        {
            this.before = before;
            this.after = after;
            this.diff = diff;
        }

        @Override
        public String toString()
        {
            return String.format("SchemaTransformationResult{%s --> %s, diff=%s}", before.getVersion(), after.getVersion(), diff);
        }
    }

    class SchemaTransformationSerializer implements MetadataSerializer<SchemaTransformation>
    {
        public void serialize(SchemaTransformation transformation, DataOutputPlus out, Version version) throws IOException
        {
            boolean hasKeyspace = transformation.keyspace() != null;
            out.writeBoolean(hasKeyspace);
            if (hasKeyspace)
                out.writeUTF(transformation.keyspace());
            out.writeUTF(transformation.cql());
        }

        public SchemaTransformation deserialize(DataInputPlus in, Version version) throws IOException
        {
            boolean hasKeyspace = in.readBoolean();
            String keyspace = null;
            if (hasKeyspace)
                keyspace = in.readUTF();
            String cql = in.readUTF();
            CQLStatement statement = QueryProcessor.getStatement(cql, ClientState.forInternalCalls(keyspace));
            if (!(statement instanceof SchemaTransformation))
                throw new IllegalArgumentException("Can not deserialize schema transformation");
            return (SchemaTransformation) statement;
        }

        public long serializedSize(SchemaTransformation t, Version version)
        {
            long size = TypeSizes.sizeof(true);
            if (t.keyspace() != null)
                size += TypeSizes.sizeof(t.keyspace());
            return size + TypeSizes.sizeof(t.cql());
        }
    }
}
