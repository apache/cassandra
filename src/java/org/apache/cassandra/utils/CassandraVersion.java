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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;

import org.apache.commons.lang3.ArrayUtils;
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
     * note: 3rd/4th groups matches to words but only allows number and checked after regexp test.
     * this is because 3rd and the last can be identical.
     **/
    private static final String VERSION_REGEXP = "(?<major>\\d+)\\.(?<minor>\\d+)(\\.(?<patch>\\w+)(\\.(?<hotfix>\\w+))?)?(-(?<prerelease>[-.\\w]+))?([.+](?<build>[.\\w]+))?";
    private static final Pattern PATTERN_WORDS = Pattern.compile("\\w+");
    @VisibleForTesting
    static final int NO_HOTFIX = -1;

    private static final Pattern PATTERN = Pattern.compile(VERSION_REGEXP);

    public static final CassandraVersion CASSANDRA_4_1 = new CassandraVersion("4.1").familyLowerBound.get();
    public static final CassandraVersion CASSANDRA_4_0 = new CassandraVersion("4.0").familyLowerBound.get();
    public static final CassandraVersion CASSANDRA_4_0_RC2 = new CassandraVersion(4, 0, 0, NO_HOTFIX, new String[] {"rc2"}, null);
    public static final CassandraVersion CASSANDRA_3_4 = new CassandraVersion("3.4").familyLowerBound.get();

    /**
     * Used to indicate that there was a previous version written to the legacy (pre 1.2)
     * system.Versions table, but that we cannot read it. Suffice to say, any upgrade should
     * proceed through 1.2.x before upgrading to the current version.
     */
    public static final CassandraVersion UNREADABLE_VERSION = new CassandraVersion("0.0.0-unknown");

    /**
     * Used to indicate that no previous version information was found. When encountered, we assume that
     * Cassandra was not previously installed and we're in the process of starting a fresh node.
     */
    public static final CassandraVersion NULL_VERSION = new CassandraVersion("0.0.0-absent");

    public final int major;
    public final int minor;
    public final int patch;
    public final int hotfix;

    public final Supplier<CassandraVersion> familyLowerBound = Suppliers.memoize(this::getFamilyLowerBound);

    private final String[] preRelease;
    private final String[] build;

    @VisibleForTesting
    CassandraVersion(int major, int minor, int patch, int hotfix, String[] preRelease, String[] build)
    {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.hotfix = hotfix;
        this.preRelease = preRelease;
        this.build = build;
    }

    /**
     * Parse a version from a string.
     *
     * @param version the string to parse
     * @throws IllegalArgumentException if the provided string does not
     *                                  represent a version
     */
    public CassandraVersion(String version)
    {
        Matcher matcher = PATTERN.matcher(version);
        if (!matcher.matches())
            throw new IllegalArgumentException("Invalid version value: " + version);

        try
        {
            this.major = intPart(matcher, "major");
            this.minor = intPart(matcher, "minor");
            this.patch = intPart(matcher, "patch", 0);
            this.hotfix = intPart(matcher, "hotfix", NO_HOTFIX);

            String pr = matcher.group("prerelease");
            String bld = matcher.group("build");

            this.preRelease = pr == null || pr.isEmpty() ? null : parseIdentifiers(version, pr);
            this.build = bld == null || bld.isEmpty() ? null : parseIdentifiers(version, bld);
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid version value: " + version, e);
        }
    }

    private static int intPart(Matcher matcher, String group)
    {
        return Integer.parseInt(matcher.group(group));
    }

    private static int intPart(Matcher matcher, String group, int orElse)
    {
        String value = matcher.group(group);
        return value == null ? orElse : Integer.parseInt(value);
    }

    private CassandraVersion getFamilyLowerBound()
    {
        return patch == 0 && hotfix == NO_HOTFIX && preRelease != null && preRelease.length == 0 && build == null
               ? this
               : new CassandraVersion(major, minor, 0, NO_HOTFIX, ArrayUtils.EMPTY_STRING_ARRAY, null);
    }

    private static String[] parseIdentifiers(String version, String str)
    {
        // Drop initial - or +
        String[] parts = StringUtils.split(str, ".-");
        for (String part : parts)
        {
            if (!PATTERN_WORDS.matcher(part).matches())
                throw new IllegalArgumentException("Invalid version value: " + version + "; " + part + " not a valid identifier");
        }
        return parts;
    }

    public List<String> getPreRelease()
    {
        return preRelease != null ? Arrays.asList(preRelease) : Collections.emptyList();
    }

    public List<String> getBuild()
    {
        return build != null ? Arrays.asList(build) : Collections.emptyList();
    }

    public int compareTo(CassandraVersion other)
    {
        return compareTo(other, false);
    }

    public int compareTo(CassandraVersion other, boolean compareToPatchOnly)
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

        if (compareToPatchOnly)
            return 0;

        int c = Integer.compare(hotfix, other.hotfix);
        if (c != 0)
            return c;

        c = compareIdentifiers(preRelease, other.preRelease, 1);
        if (c != 0)
            return c;

        return compareIdentifiers(build, other.build, -1);
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

                int c = ids1[i].compareToIgnoreCase(ids2[i]);
                if (c != 0)
                    return c;
            }
        }

        if (ids1.length < ids2.length)
        {
            // If the preRelease is empty it means that it is a family lower bound and that the first identifier is smaller than the second one
            // (e.g. 4.0.0- < 4.0.0-beta1)
            if (ids1.length == 0)
                return -1;

            // If the difference in length is only due to SNAPSHOT we know that the second identifier is smaller than the first one.
            // (e.g. 4.0.0-rc1 > 4.0.0-rc1-SNAPSHOT)
            return (ids2.length - ids1.length) == 1 && ids2[ids2.length - 1].equalsIgnoreCase("SNAPSHOT") ? 1 : -1;
        }
        if (ids1.length > ids2.length)
        {
            // If the preRelease is empty it means that it is a family lower bound and that the second identifier is smaller than the first one
            // (e.g. 4.0.0-beta1 > 4.0.0-)
            if (ids2.length == 0)
                return 1;

            // If the difference in length is only due to SNAPSHOT we know that the first identifier is smaller than the second one.
            // (e.g. 4.0.0-rc1-SNAPSHOT < 4.0.0-rc1)
            return (ids1.length - ids2.length) == 1 && ids1[ids1.length - 1].equalsIgnoreCase("SNAPSHOT") ? -1 : 1;
        }
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CassandraVersion that = (CassandraVersion) o;
        return major == that.major &&
               minor == that.minor &&
               patch == that.patch &&
               hotfix == that.hotfix &&
               Arrays.equals(preRelease, that.preRelease) &&
               Arrays.equals(build, that.build);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(major, minor, patch, hotfix);
        result = 31 * result + Arrays.hashCode(preRelease);
        result = 31 * result + Arrays.hashCode(build);
        return result;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);
        if (hotfix != NO_HOTFIX)
            sb.append('.').append(hotfix);
        if (preRelease != null)
            sb.append('-').append(StringUtils.join(preRelease, "."));
        if (build != null)
            sb.append('+').append(StringUtils.join(build, "."));
        return sb.toString();
    }
}
