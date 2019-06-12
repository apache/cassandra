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
package org.apache.cassandra.net;

import java.io.IOException;

import static java.lang.String.format;

class InvalidSerializedSizeException extends IOException
{
    final Verb verb;
    final long expectedSize;
    final long actualSizeAtLeast;

    InvalidSerializedSizeException(Verb verb, long expectedSize, long actualSizeAtLeast)
    {
        super(format("Invalid serialized size; expected %d, actual size at least %d, for verb %s", expectedSize, actualSizeAtLeast, verb));
        this.verb = verb;
        this.expectedSize = expectedSize;
        this.actualSizeAtLeast = actualSizeAtLeast;
    }

    InvalidSerializedSizeException(long expectedSize, long actualSizeAtLeast)
    {
        super(format("Invalid serialized size; expected %d, actual size at least %d", expectedSize, actualSizeAtLeast));
        this.verb = null;
        this.expectedSize = expectedSize;
        this.actualSizeAtLeast = actualSizeAtLeast;
    }
}
