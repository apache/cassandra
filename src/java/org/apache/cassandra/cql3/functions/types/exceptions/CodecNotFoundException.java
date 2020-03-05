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
package org.apache.cassandra.cql3.functions.types.exceptions;

/**
 * Thrown when a suitable {@link org.apache.cassandra.cql3.functions.types.TypeCodec} cannot be found by {@link
 * org.apache.cassandra.cql3.functions.types.CodecRegistry} instances.
 */
@SuppressWarnings("serial")
public class CodecNotFoundException extends DriverException
{

    public CodecNotFoundException(String msg)
    {
        this(msg, null);
    }

    public CodecNotFoundException(Throwable cause)
    {
        this(null, cause);
    }

    private CodecNotFoundException(String msg, Throwable cause)
    {
        super(msg, cause);
    }
}
