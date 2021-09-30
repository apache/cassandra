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

import java.util.function.BiConsumer;

import org.apache.cassandra.schema.SchemaTransformation.SchemaTransformationResult;

public interface SchemaUpdateHandlerFactory
{
    /**
     * A factory which provides the appropriate schema update handler. The actual implementation may be different for
     * different run modes (client, tool, daemon).
     *
     * @param online               whether schema update handler should work online and be aware of the other nodes (when in daemon mode)
     * @param updateSchemaCallback callback which will be called right after the shared schema is updated
     */
    SchemaUpdateHandler getSchemaUpdateHandler(boolean online, BiConsumer<SchemaTransformationResult, Boolean> updateSchemaCallback);
}
