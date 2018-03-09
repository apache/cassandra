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

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;

/**
 * Implements versioning used in Cassandra and CQL.
 * <p>
 * Note: The following code uses a slight variation from the semver document (http://semver.org).
 * </p>
 */
public class CassandraVersion implements Comparable<CassandraVersion>
{
    /**
     * note: 3rd group matches to words but only allows number and checked after regexp test.
     * this is because 3rd and the last can be identical.
     **/
    private static final String VERSION_REGEXP = "(\\d+)\\.(\\d+)(?:\\.(\\w+))?(\\-[.\\w]+)?([.+][.\\w]+)?";
    private static final Pattern PATTERN_WHITESPACE = Pattern.compile("\\w+");

    private static final Pattern pattern = Pattern.compile(VERSION_REGEXP);
    private static final Pattern SNAPSHOT = Pattern.compile("-SNAPSHOT");

    public final int major;
    public final int minor;
    public final int patch;

    private final String[] preRelease;
    private final String[] build;

    /**
     * Parse a version from a string.
     *
     * @param version the string to parse
     * @throws IllegalArgumentException if the provided string does not
     *                                  represent a version
     */
    public CassandraVersion(String version)
    {
        String stripped = SNAPSHOT.matcher(version).replaceFirst("");
        Matcher matcher = pattern.matcher(stripped);
        if (!matcher.matches())
            throw new IllegalArgumentException("Invalid version value: " + version);

        try
        {
            this.major = Integer.parseInt(matcher.group(1));
            this.minor = Integer.parseInt(matcher.group(2));
            this.patch = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;

            String pr = matcher.group(4);
            String bld = matcher.group(5);

            this.preRelease = pr == null || pr.isEmpty() ? null : parseIdentifiers(stripped, pr);
            this.build = bld == null || bld.isEmpty() ? null : parseIdentifiers(stripped, bld);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid version value: " + version, e);
        }
    }

    private static String[] parseIdentifiers(String version, String str)
    {
        // Drop initial - or +
        str = str.substring(1);
        String[] parts = StringUtils.split(str, '.');
        for (String part : parts)
        {
            if (!PATTERN_WHITESPACE.matcher(part).matches())
                throw new IllegalArgumentException("Invalid version value: " + version);
        }
        return parts;
    }

    public int compareTo(CassandraVersion other)
    {
        if (major < other.major)
            return -1;
        if (major > other.major)
            return 1;

        if (minor < other.minor)
            return -1;
        if (minor > other.minor)
            return 1;

        if (patch < other.patch)
            return -1;
        if (patch > other.patch)
            return 1;

        int c = compareIdentifiers(preRelease, other.preRelease, 1);
        if (c != 0)
            return c;

        return compareIdentifiers(build, other.build, -1);
    }

    /**
     * Returns a version that is backward compatible with this version amongst a list
     * of provided version, or null if none can be found.
     * <p>
     * For instance:
     * "2.0.0".findSupportingVersion("2.0.0", "3.0.0") == "2.0.0"
     * "2.0.0".findSupportingVersion("2.1.3", "3.0.0") == "2.1.3"
     * "2.0.0".findSupportingVersion("3.0.0") == null
     * "2.0.3".findSupportingVersion("2.0.0") == "2.0.0"
     * "2.1.0".findSupportingVersion("2.0.0") == null
     * </p>
     */
    public CassandraVersion findSupportingVersion(CassandraVersion... versions)
    {
        for (CassandraVersion version : versions)
        {
            if (isSupportedBy(version))
                return version;
        }
        return null;
    }

    public boolean isSupportedBy(CassandraVersion version)
    {
        return version != null && major == version.major && this.compareTo(version) <= 0;
    }

    private static int compareIdentifiers(String[] ids1, String[] ids2, int defaultPred)
    {
        if (ids1 == null)
            return ids2 == null ? 0 : defaultPred;
        else if (ids2 == null)
            return -defaultPred;

        int min = Math.min(ids1.length, ids2.length);
        for (int i = 0; i < min; i++)
        {
            Integer i1 = tryParseInt(ids1[i]);
            Integer i2 = tryParseInt(ids2[i]);

            if (i1 != null)
            {
                // integer have precedence
                if (i2 == null || i1 < i2)
                    return -1;
                else if (i1 > i2)
                    return 1;
            }
            else
            {
                // integer have precedence
                if (i2 != null)
                    return 1;

                int c = ids1[i].compareTo(ids2[i]);
                if (c != 0)
                    return c;
            }
        }

        if (ids1.length < ids2.length)
            return -1;
        if (ids1.length > ids2.length)
            return 1;
        return 0;
    }

    private static Integer tryParseInt(String str)
    {
        try
        {
            return Integer.valueOf(str);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof CassandraVersion))
            return false;
        CassandraVersion that = (CassandraVersion) o;
        return major == that.major
               && minor == that.minor
               && patch == that.patch
               && Arrays.equals(preRelease, that.preRelease)
               && Arrays.equals(build, that.build);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(major, minor, patch, preRelease, build);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);
        if (preRelease != null)
            sb.append('-').append(StringUtils.join(preRelease, "."));
        if (build != null)
            sb.append('+').append(StringUtils.join(build, "."));
        return sb.toString();
    }
}
