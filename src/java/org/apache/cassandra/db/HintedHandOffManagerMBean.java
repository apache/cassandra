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
package org.apache.cassandra.db;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Deprecated
public interface HintedHandOffManagerMBean
{
    /**
     * Nuke all hints from this node to `ep`.
     * @param host String rep. of endpoint address to delete hints for, either ip address ("127.0.0.1") or hostname
     */
    public void deleteHintsForEndpoint(final String host);

    /**
     *  Truncate all the hints
     */
    public void truncateAllHints() throws ExecutionException, InterruptedException;

    /**
     * List all the endpoints that this node has hints for.
     * @return set of endpoints; as Strings
     */
    public List<String> listEndpointsPendingHints();

    /** force hint delivery to an endpoint **/
    public void scheduleHintDelivery(String host) throws UnknownHostException;

    /** pause hints delivery process **/
    public void pauseHintsDelivery(boolean b);
}

