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

package org.apache.cassandra.utils;

import java.lang.reflect.Method;
import javax.security.auth.Subject;

/**
 * Listener for operations executed over JMX.
 */
public interface JmxInvocationListener
{
    /**
     * Listener called when an attempted invocation is successful.
     *
     * @param subject The subject attempting invocation, depends on how JMX authentication is configured
     * @param method Invoked method
     * @param args Invoked method arguments
     */
    default void onInvocation(Subject subject, Method method, Object[] args)
    {}

    /**
     * Listener called when an attempted invocation throws an exception. This could happen before or after the
     * underlying method is invoked, due to invocation wrappers such as {@link org.apache.cassandra.auth.jmx.AuthorizationProxy#invoke(Object, Method, Object[])}.
     *
     * @param subject The subject attempting invocation, depends on how JMX authentication is configured
     * @param method Invoked method
     * @param args Invoked method arguments
     * @param cause Exception thrown by the attempted invocation
     */
    default void onFailure(Subject subject, Method method, Object[] args, Exception cause)
    {}
}
