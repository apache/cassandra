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
package org.apache.cassandra.index.sai.disk.format;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Format version of indexing component, denoted as [major][minor]. Same forward-compatibility rules apply as to
 * {@link org.apache.cassandra.io.sstable.format.Version}.
 */
public class Version
{
    private static final Version AA = new Version('a', 'a');

    public static final Version EARLIEST = AA;
    public static final Version LATEST = AA;

    private final String version;

    public Version(char major, char minor)
    {
        this.version = major + "" + minor;
    }

    public static Version parse(String input)
    {
        checkArgument(input.length() == 2);
        return new Version(input.charAt(0), input.charAt(1));
    }

    @Override
    public String toString()
    {
        return version;
    }

    public boolean onOrAfter(Version other)
    {
        return version.compareTo(other.version) >= 0;
    }
}
