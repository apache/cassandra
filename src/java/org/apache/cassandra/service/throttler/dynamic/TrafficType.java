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

package org.apache.cassandra.service.throttler.dynamic;

public enum TrafficType {
    RangeCoordRead(false, true, true),
    NonRangeCoordRead(false, false, true),
    RangeReplicaRead(false, true, false),
    NonRangeReplicaRead(false, false, false),
    CoordWrite(true, false, true),
    ReplicaWrite(true, false, false);

    private final boolean isWrite;
    private final boolean isRangeRead;
    private final boolean isCoordTraffic;

    TrafficType(boolean isWrite, boolean isRangeRead, boolean isCoordTraffic) {
        this.isWrite = isWrite;
        this.isRangeRead = isRangeRead;
        this.isCoordTraffic = isCoordTraffic;
    }

    public boolean isWrite() {
        return isWrite;
    }

    public boolean isRangeRead() {
        return isRangeRead;
    }

    public boolean isCoordTraffic() {
        return isCoordTraffic;
    }
}
