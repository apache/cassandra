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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.util.StringTokenizer;

import com.google.common.base.Objects;

import org.apache.cassandra.io.sstable.metadata.IMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.LegacyMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.io.sstable.Component.separator;

/**
 * A SSTable is described by the keyspace and column family it contains data
 * for, a generation (where higher generations contain more recent data) and
 * an alphabetic version string.
 *
 * A descriptor can be marked as temporary, which influences generated filenames.
 */
public class Descriptor
{
    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    // Minor versions were introduced with version "hb" for Cassandra 1.0.3; prior to that,
    // we always incremented the major version.
    public static class Version
    {
        // This needs to be at the begining for initialization sake
        public static final String current_version = "ka";

        // ja (2.0.0): super columns are serialized as composites (note that there is no real format change,
        //               this is mostly a marker to know if we should expect super columns or not. We do need
        //               a major version bump however, because we should not allow streaming of super columns
        //               into this new format)
        //             tracks max local deletiontime in sstable metadata
        //             records bloom_filter_fp_chance in metadata component
        //             remove data size and column count from data file (CASSANDRA-4180)
        //             tracks max/min column values (according to comparator)
        // jb (2.0.1): switch from crc32 to adler32 for compression checksums
        //             checksum the compressed data
        // ka (2.1.0): new Statistics.db file format
        //             index summaries can be downsampled and the sampling level is persisted
        //             switch uncompressed checksums to adler32
        //             tracks presense of legacy (local and remote) counter shards

        public static final Version CURRENT = new Version(current_version);

        private final String version;

        public final boolean isLatestVersion;
        public final boolean hasPostCompressionAdlerChecksums;
        public final boolean hasSamplingLevel;
        public final boolean newStatsFile;
        public final boolean hasAllAdlerChecksums;
        public final boolean hasRepairedAt;
        public final boolean tracksLegacyCounterShards;

        public Version(String version)
        {
            this.version = version;
            isLatestVersion = version.compareTo(current_version) == 0;
            hasPostCompressionAdlerChecksums = version.compareTo("jb") >= 0;
            hasSamplingLevel = version.compareTo("ka") >= 0;
            newStatsFile = version.compareTo("ka") >= 0;
            hasAllAdlerChecksums = version.compareTo("ka") >= 0;
            hasRepairedAt = version.compareTo("ka") >= 0;
            tracksLegacyCounterShards = version.compareTo("ka") >= 0;
        }

        /**
         * @param ver SSTable version
         * @return True if the given version string matches the format.
         * @see #version
         */
        static boolean validate(String ver)
        {
            return ver != null && ver.matches("[a-z]+");
        }

        public boolean isCompatible()
        {
            return version.compareTo("ja") >= 0 && version.charAt(0) <= CURRENT.version.charAt(0);
        }

        @Override
        public String toString()
        {
            return version;
        }

        @Override
        public boolean equals(Object o)
        {
            return o == this || o instanceof Version && version.equals(((Version) o).version);
        }

        @Override
        public int hashCode()
        {
            return version.hashCode();
        }
    }

    public static enum Type
    {
        TEMP("tmp", true), TEMPLINK("tmplink", true), FINAL(null, false);
        public final boolean isTemporary;
        public final String marker;
        Type(String marker, boolean isTemporary)
        {
            this.isTemporary = isTemporary;
            this.marker = marker;
        }
    }

    public final File directory;
    /** version has the following format: <code>[a-z]+</code> */
    public final Version version;
    public final String ksname;
    public final String cfname;
    public final int generation;
    public final Type type;
    private final int hashCode;

    /**
     * A descriptor that assumes CURRENT_VERSION.
     */
    public Descriptor(File directory, String ksname, String cfname, int generation, Type temp)
    {
        this(Version.CURRENT, directory, ksname, cfname, generation, temp);
    }

    public Descriptor(String version, File directory, String ksname, String cfname, int generation, Type temp)
    {
        this(new Version(version), directory, ksname, cfname, generation, temp);
    }

    public Descriptor(Version version, File directory, String ksname, String cfname, int generation, Type temp)
    {
        assert version != null && directory != null && ksname != null && cfname != null;
        this.version = version;
        this.directory = directory;
        this.ksname = ksname;
        this.cfname = cfname;
        this.generation = generation;
        type = temp;
        hashCode = Objects.hashCode(directory, generation, ksname, cfname, temp);
    }

    public Descriptor withGeneration(int newGeneration)
    {
        return new Descriptor(version, directory, ksname, cfname, newGeneration, type);
    }

    public String filenameFor(Component component)
    {
        return filenameFor(component.name());
    }

    public String baseFilename()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(directory).append(File.separatorChar);
        appendFileName(buff);
        return buff.toString();
    }

    private void appendFileName(StringBuilder buff)
    {
        buff.append(ksname).append(separator);
        buff.append(cfname).append(separator);
        if (type.isTemporary)
            buff.append(type.marker).append(separator);
        buff.append(version).append(separator);
        buff.append(generation);
    }

    public String relativeFilenameFor(Component component)
    {
        final StringBuilder buff = new StringBuilder();
        appendFileName(buff);
        buff.append(separator).append(component.name());
        return buff.toString();
    }

    /**
     * @param suffix A component suffix, such as 'Data.db'/'Index.db'/etc
     * @return A filename for this descriptor with the given suffix.
     */
    public String filenameFor(String suffix)
    {
        return baseFilename() + separator + suffix;
    }

    /**
     * @see #fromFilename(File directory, String name)
     * @param filename The SSTable filename
     * @return Descriptor of the SSTable initialized from filename
     */
    public static Descriptor fromFilename(String filename)
    {
        File file = new File(filename);
        return fromFilename(file.getParentFile(), file.getName(), false).left;
    }

    public static Descriptor fromFilename(String filename, boolean skipComponent)
    {
        File file = new File(filename);
        return fromFilename(file.getParentFile(), file.getName(), skipComponent).left;
    }

    public static Pair<Descriptor,String> fromFilename(File directory, String name)
    {
        return fromFilename(directory, name, false);
    }

    /**
     * Filename of the form "<ksname>-<cfname>-[tmp-][<version>-]<gen>-<component>"
     *
     * @param directory The directory of the SSTable files
     * @param name The name of the SSTable file
     * @param skipComponent true if the name param should not be parsed for a component tag
     *
     * @return A Descriptor for the SSTable, and the Component remainder.
     */
    public static Pair<Descriptor,String> fromFilename(File directory, String name, boolean skipComponent)
    {
        // tokenize the filename
        StringTokenizer st = new StringTokenizer(name, String.valueOf(separator));
        String nexttok;

        // all filenames must start with keyspace and column family
        String ksname = st.nextToken();
        String cfname = st.nextToken();

        // optional temporary marker
        nexttok = st.nextToken();
        Type type = Type.FINAL;
        if (nexttok.equals(Type.TEMP.marker))
        {
            type = Type.TEMP;
            nexttok = st.nextToken();
        }
        else if (nexttok.equals(Type.TEMPLINK.marker))
        {
            type = Type.TEMPLINK;
            nexttok = st.nextToken();
        }

        if (!Version.validate(nexttok))
            throw new UnsupportedOperationException("SSTable " + name + " is too old to open.  Upgrade to 2.0 first, and run upgradesstables");
        Version version = new Version(nexttok);

        nexttok = st.nextToken();
        int generation = Integer.parseInt(nexttok);

        // component suffix
        String component = null;
        if (!skipComponent)
            component = st.nextToken();
        directory = directory != null ? directory : new File(".");
        return Pair.create(new Descriptor(version, directory, ksname, cfname, generation, type), component);
    }

    /**
     * @param type temporary flag
     * @return A clone of this descriptor with the given 'temporary' status.
     */
    public Descriptor asType(Type type)
    {
        return new Descriptor(version, directory, ksname, cfname, generation, type);
    }

    public IMetadataSerializer getMetadataSerializer()
    {
        if (version.newStatsFile)
            return new MetadataSerializer();
        else
            return new LegacyMetadataSerializer();
    }

    /**
     * @return true if the current Cassandra version can read the given sstable version
     */
    public boolean isCompatible()
    {
        return version.isCompatible();
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
        return that.directory.equals(this.directory)
                       && that.generation == this.generation
                       && that.ksname.equals(this.ksname)
                       && that.cfname.equals(this.cfname)
                       && that.type == this.type;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
