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

package org.apache.cassandra.management.api;

import java.util.Set;

/**
 * Provides execution-time services to a {@link Command}. The context acts as a typed service
 * locator so that the API package ({@code management.api}) carries no compile-time dependency
 * on tool-specific classes such as {@code NodeProbe} or {@code Output}.
 * <p>
 * Adapter layers retrieve the concrete services they need:
 * <pre>{@code
 *     NodeProbe probe = context.service(NodeProbe.class);
 *     Output    out   = context.service(Output.class);
 * }</pre>
 * New commands that do not require legacy tool types can request MBean interfaces directly:
 * <pre>{@code
 *     StorageServiceMBean ss = context.service(StorageServiceMBean.class);
 * }</pre>
 */
public interface CommandExecutionContext
{
    /**
     * Returns the service instance of the requested type, or throws
     * {@link IllegalArgumentException} if the service is not available in this context.
     *
     * @param serviceType the class of the desired service
     * @param <T> service type
     * @return a non-null service instance
     * @throws IllegalArgumentException if the service is not available
     */
    <T> T service(Class<T> serviceType);

    /**
     * Retrieves the set of all service types that are available within the current execution context.
     * @return a set of classes representing the types of services accessible via this context.
     */
    Set<Class<?>> services();
}
