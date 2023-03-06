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

package org.apache.cassandra.tcm;

import org.apache.cassandra.tcm.log.Entry;

public interface Processor
{
    /**
     * Method is _only_ responsible to commit the transformation to the cluster metadata. Implementers _have to ensure_
     * local visibility and enactment of the metadata!
     */
    Commit.Result commit(Entry.Id entryId, Transformation transform, Epoch lastKnown);
    // TODO: add a debounce to requestReplay. Right now, because of ResponseVerbHandler, it is possible to send
    // a barage of these requests.

    /**
     * Replays to the highest known epoch.
     * <p>
     * Upon replay, all items _at least_ up to returned epoch will be visible.
     */
    ClusterMetadata replayAndWait();
}
