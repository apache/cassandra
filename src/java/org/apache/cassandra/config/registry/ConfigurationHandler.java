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

package org.apache.cassandra.config.registry;

import org.slf4j.Logger;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

/**
 * Interface validating configuration property's value.
 */
public interface ConfigurationHandler
{
    /**
     * Called before a property change occurrs. If this method throws an exception, the change
     * will be aborted. If it returns normally, the change will proceed with a returned value.
     *
     * @param config the current configuration.
     * @param descriptor the descriptor of the property to be changed.
     * @param logger the logger to use for logging.
     */
    void validate(Config config, DatabaseDescriptor.DynamicDatabaseDescriptor descriptor, Logger logger);
}
