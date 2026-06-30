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

package org.apache.cassandra.tools.nodetool;

/**
 * Marker interface for nodetool commands that are known to run entirely locally,
 * without requiring a JMX connection to the node.
 * <p>
 * Commands that implement this interface will not display JMX connection options
 * (host, port, username, password, etc.) in their help output, since those options
 * are not applicable to local commands.
 * <p>
 * Note: this is different from commands whose locality is determined at runtime
 * (e.g. {@link Sjk}), which override {@link AbstractCommand#shouldConnect()} instead.
 */
public interface LocalCommand
{
}
