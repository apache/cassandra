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

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Update handler which works only in memory. It does not load or save the schema anywhere. It is used in client mode
 * applications.
 */
public class OfflineSchemaUpdateHandler implements SchemaUpdateHandler
{
    private static final Logger logger = LoggerFactory.getLogger(OfflineSchemaUpdateHandler.class);

    private final Consumer<SchemaTransformationResult> updateCallback;

    private volatile SharedSchema schema = SharedSchema.EMPTY;

    public OfflineSchemaUpdateHandler(Consumer<SchemaTransformationResult> updateCallback)
    {
        this.updateCallback = updateCallback;
    }

    @Override
    public void start()
    {
        // no-op
    }

    @Override
    public boolean waitUntilReady(Duration timeout)
    {
        return true;
    }

    @Override
    public synchronized SchemaTransformationResult apply(SchemaTransformation transformation, boolean local)
    {
        SharedSchema before = schema;
        Keyspaces afterKeyspaces = transformation.apply(before.getKeyspaces());
        Keyspaces.KeyspacesDiff diff = Keyspaces.diff(before.getKeyspaces(), afterKeyspaces);

        if (diff.isEmpty())
            return new SchemaTransformationResult(before, before, diff, Collections.emptyList());

        SharedSchema after = new SharedSchema(afterKeyspaces, UUID.nameUUIDFromBytes(ByteArrayUtil.bytes(afterKeyspaces.hashCode())));
        SchemaTransformationResult update = new SchemaTransformationResult(before, after, diff, SchemaKeyspace.convertSchemaDiffToMutations(diff, transformation.fixedTimestampMicros().orElse(FBUtilities.timestampMicros())));
        this.schema = after;
        logger.debug("Schema updated: {}", update);
        updateCallback.accept(update);

        return update;
    }

    @Override
    public SchemaTransformationResult reset(boolean local)
    {
        if (!local)
            throw new UnsupportedOperationException();

        return apply(ignored -> SchemaKeyspace.fetchNonSystemKeyspaces(), local);
    }

    @Override
    public synchronized void clear()
    {
        this.schema = SharedSchema.EMPTY;
    }
}
