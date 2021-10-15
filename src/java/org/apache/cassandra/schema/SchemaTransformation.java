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

import java.util.Collection;

import org.apache.cassandra.db.Mutation;

public interface SchemaTransformation
{
    /**
     * Apply a statement transformation to a schema snapshot.
     * <p>
     * Implementing methods should be side-effect free (outside of throwing exceptions if the transformation cannot
     * be successfully applied to the provided schema).
     *
     * @param schema Keyspaces to base the transformation on
     * @return Keyspaces transformed by the statement
     */
    Keyspaces apply(Keyspaces schema);

    /**
     * The result of applying (on this node) a given schema transformation.
     */
    class SchemaTransformationResult
    {
        public final SharedSchema before;
        public final SharedSchema after;
        public final Keyspaces.KeyspacesDiff diff;
        public final Collection<Mutation> mutations;

        public SchemaTransformationResult(SharedSchema before, SharedSchema after, Keyspaces.KeyspacesDiff diff, Collection<Mutation> mutations)
        {
            this.before = before;
            this.after = after;
            this.diff = diff;
            this.mutations = mutations;
        }

        @Override
        public String toString()
        {
            return String.format("SchemaTransformationResult{diff=%s}", diff);
        }
    }
}
