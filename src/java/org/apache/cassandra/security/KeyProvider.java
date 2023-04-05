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
package org.apache.cassandra.security;

import java.io.IOException;
import java.security.Key;

/**
 * Customizable key retrieval mechanism. Implementations should expect that retrieved keys will be cached.
 * Further, each key will be requested non-concurrently (that is, no stampeding herds for the same key), although
 * unique keys may be requested concurrently (unless you mark {@code getSecretKey} synchronized).
 *
 * Implementations must provide a constructor that accepts {@code TransparentDataEncryptionOptions} as the sole parameter.
 */
public interface KeyProvider
{
    Key getSecretKey(String alias) throws IOException;
}
