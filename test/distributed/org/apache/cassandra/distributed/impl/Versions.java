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

package org.apache.cassandra.distributed.impl;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.utils.FBUtilities;

public class Versions
{
    private static final Logger logger = LoggerFactory.getLogger(Versions.class);
    public static Version CURRENT = new Version(FBUtilities.getReleaseVersionString(), ((URLClassLoader)Versions.class.getClassLoader()).getURLs());

    public enum Major
    {
        v22("2\\.2\\.([0-9]+)"),
        v30("3\\.0\\.([0-9]+)"),
        v3X("3\\.([1-9]|1[01])(\\.([0-9]+))?"),
        v4("4\\.([0-9]+)");
        final Pattern pattern;
        Major(String verify)
        {
            this.pattern = Pattern.compile(verify);
        }

        static Major fromFull(String version)
        {
            if (version.isEmpty())
                throw new IllegalArgumentException(version);
            switch (version.charAt(0))
            {
                case '2':
                    if (version.startsWith("2.2"))
                        return v22;
                    throw new IllegalArgumentException(version);
                case '3':
                    if (version.startsWith("3.0"))
                        return v30;
                    return v3X;
                case '4':
                    return v4;
                default:
                    throw new IllegalArgumentException(version);
            }
        }

        // verify that the version string is valid for this major version
        boolean verify(String version)
        {
            return pattern.matcher(version).matches();
        }

        // sort two strings of the same major version as this enum instance
        int compare(String a, String b)
        {
            Matcher ma = pattern.matcher(a);
            Matcher mb = pattern.matcher(a);
            if (!ma.matches()) throw new IllegalArgumentException(a);
            if (!mb.matches()) throw new IllegalArgumentException(b);
            int result = Integer.compare(Integer.parseInt(ma.group(1)), Integer.parseInt(mb.group(1)));
            if (result == 0 && this == v3X && (ma.group(3) != null || mb.group(3) != null))
            {
                if (ma.group(3) != null && mb.group(3) != null)
                {
                    result = Integer.compare(Integer.parseInt(ma.group(3)), Integer.parseInt(mb.group(3)));
                }
                else
                {
                    result = ma.group(3) != null ? 1 : -1;
                }
            }
            // sort descending
            return -result;
        }
    }

    public static class Version
    {
        public final Major major;
        public final String version;
        public final URL[] classpath;

        public Version(String version, URL[] classpath)
        {
            this(Major.fromFull(version), version, classpath);
        }
        public Version(Major major, String version, URL[] classpath)
        {
            this.major = major;
            this.version = version;
            this.classpath = classpath;
        }
    }

    private final Map<Major, List<Version>> versions;
    public Versions(Map<Major, List<Version>> versions)
    {
        this.versions = versions;
    }

    public Version get(String full)
    {
        return versions.get(Major.fromFull(full))
                       .stream()
                       .filter(v -> full.equals(v.version))
                       .findFirst()
                       .orElseThrow(() -> new RuntimeException("No version " + full + " found"));
    }

    public Version getLatest(Major major)
    {
        return versions.get(major).stream().findFirst().orElseThrow(() -> new RuntimeException("No " + major + " versions found"));
    }

    public static Versions find()
    {
        final String dtestJarDirectory = System.getProperty(Config.PROPERTY_PREFIX + "test.dtest_jar_path","build");
        final File sourceDirectory = new File(dtestJarDirectory);
        logger.info("Looking for dtest jars in " + sourceDirectory.getAbsolutePath());
        final Pattern pattern = Pattern.compile("dtest-(?<fullversion>(\\d+)\\.(\\d+)(\\.\\d+)?(\\.\\d+)?)([~\\-]\\w[.\\w]*(?:\\-\\w[.\\w]*)*)?(\\+[.\\w]+)?\\.jar");
        final Map<Major, List<Version>> versions = new HashMap<>();
        for (Major major : Major.values())
            versions.put(major, new ArrayList<>());

        for (File file : sourceDirectory.listFiles())
        {
            Matcher m = pattern.matcher(file.getName());
            if (!m.matches())
                continue;
            String version = m.group("fullversion");
            Major major = Major.fromFull(version);
            versions.get(major).add(new Version(major, version, new URL[] { toURL(file) }));
        }

        for (Map.Entry<Major, List<Version>> e : versions.entrySet())
        {
            if (e.getValue().isEmpty())
                continue;
            Collections.sort(e.getValue(), Comparator.comparing(v -> v.version, e.getKey()::compare));
            logger.info("Found " + e.getValue().stream().map(v -> v.version).collect(Collectors.joining(", ")));
        }

        return new Versions(versions);
    }

    public static URL toURL(File file)
    {
        try
        {
            return file.toURI().toURL();
        }
        catch (MalformedURLException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

}
