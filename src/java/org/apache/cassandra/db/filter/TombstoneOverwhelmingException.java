/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.db.filter;

import org.apache.cassandra.db.DecoratedKey;


public class TombstoneOverwhelmingException extends RuntimeException
{
    private final int numTombstones;
    private final int numRequested;
    private final String ksName;
    private final String cfName;
    private final String lastCellName;
    private final String slicesInfo;
    private final String deletionInfo;
    private String partitionKey = null;

    public TombstoneOverwhelmingException(int numTombstones, int numRequested, String ksName, String cfName,
                                          String lastCellName, String slicesInfo, String deletionInfo)
    {
        this.numTombstones = numTombstones;
        this.numRequested = numRequested;
        this.ksName = ksName;
        this.cfName = cfName;
        this.lastCellName = lastCellName;
        this.slicesInfo = slicesInfo;
        this.deletionInfo = deletionInfo;
    }

    public void setKey(DecoratedKey key)
    {
        if(key != null)
            this.partitionKey = key.toString();
    }

    public String getLocalizedMessage()
    {
        return getMessage();
    }

    public String getMessage()
    {
        return String.format(
                "Scanned over %d tombstones in %s.%s; %d columns were requested; query aborted " +
                "(see tombstone_failure_threshold); partitionKey=%s; lastCell=%s; delInfo=%s; slices=%s",
                numTombstones, ksName, cfName, numRequested, partitionKey, lastCellName, deletionInfo, slicesInfo);
    }
}
