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
package org.apache.cassandra.journal;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.io.util.File;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

/**
 * Timestamp and version encoded in the file name, e.g.
 * log-1637159888484-2-1-1.data
 * log-1637159888484-2-1-1.indx
 * log-1637159888484-2-1-1.meta
 * log-1637159888484-2-1-1.sync
 */
public final class Descriptor implements Comparable<Descriptor>
{
    private static final String SEPARATOR = "-";
    private static final String PREFIX = "log" + SEPARATOR;
    private static final String TMP_SUFFIX = "tmp";

    private static final Pattern DATA_FILE_PATTERN =
        Pattern.compile(   PREFIX + "(\\d+)" // timestamp
                      + SEPARATOR + "(\\d+)" // generation
                      + SEPARATOR + "(\\d+)" // journal version
                      + SEPARATOR + "(\\d+)" // user version
                      + "\\."     + Component.DATA.extension);

    private static final Pattern TMP_FILE_PATTERN =
        Pattern.compile(   PREFIX + "\\d+"   // timestamp
                      + SEPARATOR + "\\d+"   // generation
                      + SEPARATOR + "\\d+"   // journal version
                      + SEPARATOR + "\\d+"   // user version
                      + "\\."     + "[a-z]+" // component extension
                      + "\\."     + TMP_SUFFIX);


    /*
     * NOTE: If and when another journal version is introduced, have implementations
     * expose the version used via yaml. This way operators can force previous journal
     * version on upgrade, temporarily, to allow easier downgrades if something goes wrong.
     */
    static final int JOURNAL_VERSION_1 = 1;
    static final int CURRENT_JOURNAL_VERSION = JOURNAL_VERSION_1;

    final File directory;
    final long timestamp;
    final int generation;

    /**
     * Serialization version for journal components; bumped as journal
     * implementation evolves over time.
     */
    final int journalVersion;

    /**
     * Serialization version for user content - specifically journal keys
     * and journal values; bumped when user logic evolves.
     */
    final int userVersion;

    Descriptor(File directory, long timestamp, int generation, int journalVersion, int userVersion)
    {
        this.directory = directory;
        this.timestamp = timestamp;
        this.generation = generation;
        this.journalVersion = journalVersion;
        this.userVersion = userVersion;
    }

    static Descriptor create(File directory, long timestamp, int userVersion)
    {
        return new Descriptor(directory, timestamp, 1, CURRENT_JOURNAL_VERSION, userVersion);
    }

    static Descriptor fromName(File directory, String name)
    {
        Matcher matcher = DATA_FILE_PATTERN.matcher(name);
        if (!matcher.matches())
            throw new IllegalArgumentException("Provided filename " + new File(directory, name) + " is not valid for a data segment file");

        long timestamp = Long.parseLong(matcher.group(1));
        int generation = Integer.parseInt(matcher.group(2));
        int journalVersion = Integer.parseInt(matcher.group(3));
        int userVersion = Integer.parseInt(matcher.group(4));

        return new Descriptor(directory, timestamp, generation, journalVersion, userVersion);
    }

    static Descriptor fromFile(File file)
    {
        return fromName(file.parent(), file.name());
    }

    Descriptor withIncrementedGeneration()
    {
        return new Descriptor(directory, timestamp, generation + 1, journalVersion, userVersion);
    }

    File fileFor(Component component)
    {
        return new File(directory, formatFileName(component));
    }

    File tmpFileFor(Component component)
    {
        return new File(directory, formatFileName(component) + '.' + TMP_SUFFIX);
    }

    static boolean isTmpFile(File file)
    {
        return TMP_FILE_PATTERN.matcher(file.name()).matches();
    }

    private String formatFileName(Component component)
    {
        return format("%s%d%s%d%s%d%s%d.%s",
                      PREFIX, timestamp,
                      SEPARATOR, generation,
                      SEPARATOR, journalVersion,
                      SEPARATOR, userVersion,
                      component.extension);
    }

    static List<Descriptor> list(File directory)
    {
        try
        {
            return Arrays.stream(directory.listNames((file, name) -> DATA_FILE_PATTERN.matcher(name).matches()))
                         .map(name -> fromName(directory, name))
                         .collect(toList());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int compareTo(Descriptor other)
    {
        assert this.directory.equals(other.directory)
             : format("Descriptors have mismatching directories: %s and %s", this.directory, other.directory);

                  int cmp = Long.compare(this.timestamp, other.timestamp);
        if (cmp == 0) cmp = Integer.compare(this.generation, other.generation);
        if (cmp == 0) cmp = Integer.compare(this.journalVersion, other.journalVersion);
        if (cmp == 0) cmp = Integer.compare(this.userVersion, other.userVersion);
        return cmp;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;
        return (other instanceof Descriptor) && equals((Descriptor) other);
    }

    boolean equals(Descriptor other)
    {
        assert this.directory.equals(other.directory)
             : format("Descriptors have mismatching directories: %s and %s", this.directory, other.directory);

        return this.timestamp == other.timestamp
            && this.generation == other.generation
            && this.journalVersion == other.journalVersion
            && this.userVersion == other.userVersion;
    }

    @Override
    public int hashCode()
    {
        int result = directory.hashCode();
        result = 31 * result + Long.hashCode(timestamp);
        result = 31 * result + generation;
        result = 31 * result + journalVersion;
        result = 31 * result + userVersion;
        return result;
    }

    @Override
    public String toString()
    {
        return format("dir: %s, ts: %d, gen: %d, journal ver: %d, user ver: %d",
                      directory, timestamp, generation, journalVersion, userVersion);
    }
}
