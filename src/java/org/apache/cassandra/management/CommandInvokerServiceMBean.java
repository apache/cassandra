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

package org.apache.cassandra.management;

/**
 * MBean interface for CommandInvokerService.
 * Exposes registry-level operations (not individual commands).
 */
public interface CommandInvokerServiceMBean
{
    String MBEAN_NAME = "org.apache.cassandra.management:type=CommandInvokerService";

    /**
     * Get a list of all command names in the registry.
     * @return array of command names
     */
    String[] getCommandNames();

    /**
     * Get the total number of commands in the registry.
     * @return number of commands
     */
    int getCommandCount();

    /**
     * Get ObjectName string for a specific command MBean.
     * @param fullCommandName name of the command
     * @return ObjectName string for the command MBean
     * @throws IllegalArgumentException if command not found
     */
    String getCommandMBeanName(String fullCommandName);
}
