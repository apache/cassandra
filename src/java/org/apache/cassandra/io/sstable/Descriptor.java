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

import org.apache.cassandra.db.Table;
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
    // we always incremented the major version.  In particular, versions g and h are
    // forwards-compatible with version f, so if the above convention had been followed,
    // we would have labeled them fb and fc.
    public static final String LEGACY_VERSION = "a"; // "pre-history"
    // b (0.7.0): added version to sstable filenames
    // c (0.7.0): bloom filter component computes hashes over raw key bytes instead of strings
    // d (0.7.0): row size in data component becomes a long instead of int
    // e (0.7.0): stores undecorated keys in data and index components
    // f (0.7.0): switched bloom filter implementations in data component
    // g (0.8): tracks flushed-at context in metadata component
    // h (1.0): tracks max client timestamp in metadata component
    // hb (1.0.3): records compression ration in metadata component
    // hc (1.0.4): records partitioner in metadata component
    public static final String CURRENT_VERSION = "hc";

    public final File directory;
    /** version has the following format: <code>[a-z]+</code> */
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
    public final boolean metadataIncludesReplayPosition;
    public final boolean tracksMaxTimestamp;
    public final boolean hasCompressionRatio;
    public final boolean hasPartitioner;

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
        usesOldBloomFilter = version.compareTo("f") < 0;
        metadataIncludesReplayPosition = version.compareTo("g") >= 0;
        tracksMaxTimestamp = version.compareTo("h") >= 0;
        hasCompressionRatio = version.compareTo("hb") >= 0;
        hasPartitioner = version.compareTo("hc") >= 0;
        isLatestVersion = version.compareTo(CURRENT_VERSION) == 0;
    }

    public String filenameFor(Component component)
    {
        return filenameFor(component.name());
    }
    
    private String baseFilename()
    {
        StringBuilder buff = new StringBuilder();
        buff.append(directory).append(File.separatorChar);
        buff.append(cfname).append(separator);
        if (temporary)
            buff.append(SSTable.TEMPFILE_MARKER).append(separator);
        if (!LEGACY_VERSION.equals(version))
            buff.append(version).append(separator);
        buff.append(generation);
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
        assert file.getParentFile() != null : "Filename must include parent directory.";
        return fromFilename(file.getParentFile(), file.getName()).left;
    }

    /**
     * Filename of the form "<ksname>/<cfname>-[tmp-][<version>-]<gen>-<component>"
     *
     * @param directory The directory of the SSTable files
     * @param name The name of the SSTable file
     *
     * @return A Descriptor for the SSTable, and the Component remainder.
     */
    public static Pair<Descriptor,String> fromFilename(File directory, String name)
    {
        // name of parent directory is keyspace name
        String ksname = extractKeyspaceName(directory);

        // tokenize the filename
        StringTokenizer st = new StringTokenizer(name, String.valueOf(separator));
        String nexttok;

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
     * Extracts the keyspace name out of the directory name. Snapshot directories have a slightly different
     * path structure and need to be treated differently.
     *
     * Regular path:   "<ksname>/<cfname>-[tmp-][<version>-]<gen>-<component>"
     * Snapshot path: "<ksname>/snapshots/<snapshot-name>/<cfname>-[tmp-][<version>-]<gen>-<component>"
     *
     * @param directory a directory containing SSTables
     * @return the keyspace name
     */
    public static String extractKeyspaceName(File directory)
    {
        if (isSnapshotInPath(directory))
        {
            // We need to move backwards. If this is a snapshot, first parent takes us to:
            // <ksname>/snapshots/ and second call to parent takes us to <ksname>.
            return directory.getParentFile().getParentFile().getName();
        }
        return directory.getName();
    }

    /**
     * @param directory The directory to check
     * @return <code>TRUE</code> if this directory represents a snapshot directory. <code>FALSE</code> otherwise.
     */
    private static boolean isSnapshotInPath(File directory)
    {
        File curDirectory = directory;
        while (curDirectory != null)
        {
            if (curDirectory.getName().equals(Table.SNAPSHOT_SUBDIR_NAME))
                return true;
            curDirectory = curDirectory.getParentFile();
        }

        // The directory does not represent a snapshot directory.
        return false;
    }

    /**
     * @param temporary temporary flag
     * @return A clone of this descriptor with the given 'temporary' status.
     */
    public Descriptor asTemporary(boolean temporary)
    {
        return new Descriptor(version, directory, ksname, cfname, generation, temporary);
    }

    /**
     * @param ver SSTable version
     * @return True if the given version string matches the format.
     * @see #version
     */
    static boolean versionValidate(String ver)
    {
        return ver != null && ver.matches("[a-z]+");
    }

    /**
     * @return true if the current Cassandra version can read the given sstable version
     */
    public boolean isCompatible()
    {
        return version.charAt(0) <= CURRENT_VERSION.charAt(0);
    }

    /**
     * @return true if the current Cassandra version can stream the given sstable version
     * from another node.  This is stricter than opening it locally [isCompatible] because
     * streaming needs to rebuild all the non-data components, and it only knows how to write
     * the latest version.
     */
    public boolean isStreamCompatible()
    {
        // we could add compatibility for earlier versions with the new single-pass streaming
        // (see SSTableWriter.appendFromStream) but versions earlier than 0.7.1 don't have the
        // MessagingService version awareness anyway so there's no point.
        return isCompatible() && version.charAt(0) >= 'f';
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
