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

import java.util.List;

import com.google.common.collect.ImmutableList;

public class DiskBoundaries
{
    public final List<Directories.DataDirectory> directories;
    public final ImmutableList<PartitionPosition> positions;
    final long ringVersion;
    final int directoriesVersion;

    DiskBoundaries(Directories.DataDirectory[] directories, List<PartitionPosition> positions, long ringVersion, int diskVersion)
    {
        this.directories = directories == null ? null : ImmutableList.copyOf(directories);
        this.positions = positions == null ? null : ImmutableList.copyOf(positions);
        this.ringVersion = ringVersion;
        this.directoriesVersion = diskVersion;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DiskBoundaries that = (DiskBoundaries) o;

        if (ringVersion != that.ringVersion) return false;
        if (directoriesVersion != that.directoriesVersion) return false;
        if (!directories.equals(that.directories)) return false;
        return positions != null ? positions.equals(that.positions) : that.positions == null;
    }

    public int hashCode()
    {
        int result = directories != null ? directories.hashCode() : 0;
        result = 31 * result + (positions != null ? positions.hashCode() : 0);
        result = 31 * result + (int) (ringVersion ^ (ringVersion >>> 32));
        result = 31 * result + directoriesVersion;
        return result;
    }

    public String toString()
    {
        return "DiskBoundaries{" +
               "directories=" + directories +
               ", positions=" + positions +
               ", ringVersion=" + ringVersion +
               ", directoriesVersion=" + directoriesVersion +
               '}';
    }
}