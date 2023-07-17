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

import org.apache.cassandra.exceptions.StartupException;

/**
 * Interface for managing the cryptographic provider
 */
public interface ICryptoProvider
{

    /**
     * Installs a cryptographic provider of a specific type.
     * @throws StartupException if an error occurs while installing the provider
     */
    public void installProvider() throws StartupException;

    /**
     * Checks the status of the provider
     * @throws Exception if an error occurs while checking the provider's status or the provider is not installed properly.
     */
    public void checkProvider() throws Exception;
}
