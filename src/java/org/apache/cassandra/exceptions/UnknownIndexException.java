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

package org.apache.cassandra.exceptions;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.schema.TableMetadata;

/**
 * Exception thrown when we read an index id from a serialized ReadCommand and no corresponding IndexMetadata
 * can be found in the TableMetadata#indexes collection. Note that this is an internal exception and is not meant
 * to be user facing, the node reading the ReadCommand should proceed as if no index id were present.
 */
public final class UnknownIndexException extends IOException
{
    public final UUID indexId;
    public UnknownIndexException(TableMetadata metadata, UUID id)
    {
        super(String.format("Unknown index %s for table %s", id.toString(), metadata.toString()));
        indexId = id;
    }
}
