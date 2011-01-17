package org.apache.cassandra.io.sstable;
/*
 * 
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
 * 
 */


import java.io.File;
import java.util.StringTokenizer;

import com.google.common.base.Objects;

import org.apache.cassandra.utils.Pair;

/**
 * A SSTable is described by the keyspace and column family it contains data
 * for, a generation (where higher generations contain more recent data) and
 * an alphabetic version string.
 *
 * A descriptor can be marked as temporary, which influences generated filenames.
 */
public class Descriptor
{
    public static final String LEGACY_VERSION = "a";
    public static final String CURRENT_VERSION = "f";

    public final File directory;
    public final String version;
    public final String ksname;
    public final String cfname;
    public final int generation;
    public final boolean temporary;
    private final int hashCode;

    public final boolean hasStringsInBloomFilter;
    public final boolean hasIntRowSize;
    public final boolean hasEncodedKeys;
    public final boolean isLatestVersion;
    public final boolean usesOldBloomFilter;

    /**
     * A descriptor that assumes CURRENT_VERSION.
     */
    public Descriptor(File directory, String ksname, String cfname, int generation, boolean temp)
    {
        this(CURRENT_VERSION, directory, ksname, cfname, generation, temp);
    }

    public Descriptor(String version, File directory, String ksname, String cfname, int generation, boolean temp)
    {
        assert version != null && directory != null && ksname != null && cfname != null;
        this.version = version;
        this.directory = directory;
        this.ksname = ksname;
        this.cfname = cfname;
        this.generation = generation;
        temporary = temp;
        hashCode = Objects.hashCode(directory, generation, ksname, cfname);

        hasStringsInBloomFilter = version.compareTo("c") < 0;
        hasIntRowSize = version.compareTo("d") < 0;
        hasEncodedKeys = version.compareTo("e") < 0;
        isLatestVersion = version.compareTo(CURRENT_VERSION) == 0;
        usesOldBloomFilter = version.compareTo("f") < 0;
    }

    public String filenameFor(Component component)
    {
        return filenameFor(component.name());
    }
    
    private String baseFilename()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(directory).append(File.separatorChar);
        buff.append(cfname).append("-");
        if (temporary)
            buff.append(SSTable.TEMPFILE_MARKER).append("-");
        if (!LEGACY_VERSION.equals(version))
            buff.append(version).append("-");
        buff.append(generation);
        return buff.toString();
    }

    /**
     * @param suffix A component suffix, such as 'Data.db'/'Index.db'/etc
     * @return A filename for this descriptor with the given suffix.
     */
    public String filenameFor(String suffix)
    {
        return baseFilename() + "-" + suffix;
    }

    /**
     * @see #fromFilename(File directory, String name)
     */
    public static Descriptor fromFilename(String filename)
    {
        int separatorPos = filename.lastIndexOf(File.separatorChar);
        assert separatorPos != -1 : "Filename must include parent directory.";
        File directory = new File(filename.substring(0, separatorPos));
        String name = filename.substring(separatorPos+1, filename.length());
        return fromFilename(directory, name).left;
    }

    /**
     * Filename of the form "<ksname>/<cfname>-[tmp-][<version>-]<gen>-<component>"
     * @return A Descriptor for the SSTable, and the Component remainder.
     */
    public static Pair<Descriptor,String> fromFilename(File directory, String name)
    {
        // name of parent directory is keyspace name
        String ksname = directory.getName();

        // tokenize the filename
        StringTokenizer st = new StringTokenizer(name, "-");
        String nexttok = null;

        // all filenames must start with a column family
        String cfname = st.nextToken();

        // optional temporary marker
        nexttok = st.nextToken();
        boolean temporary = false;
        if (nexttok.equals(SSTable.TEMPFILE_MARKER))
        {
            temporary = true;
            nexttok = st.nextToken();
        }

        // optional version string
        String version = LEGACY_VERSION;
        if (versionValidate(nexttok))
        {
            version = nexttok;
            nexttok = st.nextToken();
        }
        int generation = Integer.parseInt(nexttok);

        // component suffix
        String component = st.nextToken();

        return new Pair<Descriptor,String>(new Descriptor(version, directory, ksname, cfname, generation, temporary), component);
    }

    /**
     * @return A clone of this descriptor with the given 'temporary' status.
     */
    public Descriptor asTemporary(boolean temporary)
    {
        return new Descriptor(version, directory, ksname, cfname, generation, temporary);
    }

    /**
     * @return True if the given version string is not empty, and
     * contains all lowercase letters, as defined by java.lang.Character.
     */
    static boolean versionValidate(String ver)
    {
        if (ver.length() < 1) return false;
        for (char ch : ver.toCharArray())
            if (!Character.isLetter(ch) || !Character.isLowerCase(ch))
                return false;
        return true;
    }

    public boolean isFromTheFuture()
    {
        return version.compareTo(CURRENT_VERSION) > 0;
    }

    @Override
    public String toString()
    {
        return baseFilename();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this)
            return true;
        if (!(o instanceof Descriptor))
            return false;
        Descriptor that = (Descriptor)o;
        return that.directory.equals(this.directory) && that.generation == this.generation && that.ksname.equals(this.ksname) && that.cfname.equals(this.cfname);
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
