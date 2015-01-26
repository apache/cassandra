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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.StringTokenizer;

import com.google.common.base.CharMatcher;
import com.google.common.base.Objects;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
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
    public final SSTableFormat.Type formatType;
    private final int hashCode;

    /**
     * A descriptor that assumes CURRENT_VERSION.
     */
    public Descriptor(File directory, String ksname, String cfname, int generation, Type temp)
    {
        this(DatabaseDescriptor.getSSTableFormat().info.getLatestVersion(), directory, ksname, cfname, generation, temp, DatabaseDescriptor.getSSTableFormat());
    }

    public Descriptor(File directory, String ksname, String cfname, int generation, Type temp, SSTableFormat.Type formatType)
    {
        this(formatType.info.getLatestVersion(), directory, ksname, cfname, generation, temp, formatType);
    }

    public Descriptor(String version, File directory, String ksname, String cfname, int generation, Type temp, SSTableFormat.Type formatType)
    {
        this(formatType.info.getVersion(version), directory, ksname, cfname, generation, temp, formatType);
    }

    public Descriptor(Version version, File directory, String ksname, String cfname, int generation, Type temp, SSTableFormat.Type formatType)
    {
        assert version != null && directory != null && ksname != null && cfname != null && formatType.info.getLatestVersion().getClass().equals(version.getClass());
        this.version = version;
        this.directory = directory;
        this.ksname = ksname;
        this.cfname = cfname;
        this.generation = generation;
        this.type = temp;
        this.formatType = formatType;

        hashCode = Objects.hashCode(version, directory, generation, ksname, cfname, temp, formatType);
    }

    public Descriptor withGeneration(int newGeneration)
    {
        return new Descriptor(version, directory, ksname, cfname, newGeneration, type, formatType);
    }

    public Descriptor withFormatType(SSTableFormat.Type newType)
    {
        return new Descriptor(newType.info.getLatestVersion(), directory, ksname, cfname, generation, type, newType);
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
        if (!version.hasNewFileName())
        {
            buff.append(ksname).append(separator);
            buff.append(cfname).append(separator);
        }
        if (type.isTemporary)
            buff.append(type.marker).append(separator);
        buff.append(version).append(separator);
        buff.append(generation);
        if (formatType != SSTableFormat.Type.LEGACY)
            buff.append(separator).append(formatType.name);
    }

    public String relativeFilenameFor(Component component)
    {
        final StringBuilder buff = new StringBuilder();
        appendFileName(buff);
        buff.append(separator).append(component.name());
        return buff.toString();
    }

    public SSTableFormat getFormat()
    {
        return formatType.info;
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

    public static Descriptor fromFilename(String filename, SSTableFormat.Type formatType)
    {
        return fromFilename(filename).withFormatType(formatType);
    }

    public static Descriptor fromFilename(String filename, boolean skipComponent)
    {
        File file = new File(filename);
        return fromFilename(file.getParentFile(), file.getName(), skipComponent).left;
    }

    public static Pair<Descriptor, String> fromFilename(File directory, String name)
    {
        return fromFilename(directory, name, false);
    }

    /**
     * Filename of the form is vary by version:
     *
     * <ul>
     *     <li>&lt;ksname&gt;-&lt;cfname&gt;-(tmp-)?&lt;version&gt;-&lt;gen&gt;-&lt;component&gt; for cassandra 2.0 and before</li>
     *     <li>(&lt;tmp marker&gt;-)?&lt;version&gt;-&lt;gen&gt;-&lt;component&gt; for cassandra 3.0 and later</li>
     * </ul>
     *
     * If this is for SSTable of secondary index, directory should ends with index name for 2.1+.
     *
     * @param directory The directory of the SSTable files
     * @param name The name of the SSTable file
     * @param skipComponent true if the name param should not be parsed for a component tag
     *
     * @return A Descriptor for the SSTable, and the Component remainder.
     */
    public static Pair<Descriptor, String> fromFilename(File directory, String name, boolean skipComponent)
    {
        File parentDirectory = directory != null ? directory : new File(".");

        // tokenize the filename
        StringTokenizer st = new StringTokenizer(name, String.valueOf(separator));
        String nexttok;

        // read tokens backwards to determine version
        Deque<String> tokenStack = new ArrayDeque<>();
        while (st.hasMoreTokens())
        {
            tokenStack.push(st.nextToken());
        }

        // component suffix
        String component = skipComponent ? null : tokenStack.pop();

        nexttok = tokenStack.pop();
        // generation OR Type
        SSTableFormat.Type fmt = SSTableFormat.Type.LEGACY;
        if (!CharMatcher.DIGIT.matchesAllOf(nexttok))
        {
            fmt = SSTableFormat.Type.validate(nexttok);
            nexttok = tokenStack.pop();
        }

        // generation
        int generation = Integer.parseInt(nexttok);

        // version
        nexttok = tokenStack.pop();
        Version version = fmt.info.getVersion(nexttok);

        if (!version.validate(nexttok))
            throw new UnsupportedOperationException("SSTable " + name + " is too old to open.  Upgrade to 2.0 first, and run upgradesstables");

        // optional temporary marker
        Type type = Descriptor.Type.FINAL;
        nexttok = tokenStack.peek();
        if (Descriptor.Type.TEMP.marker.equals(nexttok))
        {
            type = Descriptor.Type.TEMP;
            tokenStack.pop();
        }
        else if (Descriptor.Type.TEMPLINK.marker.equals(nexttok))
        {
            type = Descriptor.Type.TEMPLINK;
            tokenStack.pop();
        }

        // ks/cf names
        String ksname, cfname;
        if (version.hasNewFileName())
        {
            // for 2.1+ read ks and cf names from directory
            File cfDirectory = parentDirectory;
            // check if this is secondary index
            String indexName = "";
            if (cfDirectory.getName().startsWith(Directories.SECONDARY_INDEX_NAME_SEPARATOR))
            {
                indexName = cfDirectory.getName();
                cfDirectory = cfDirectory.getParentFile();
            }
            if (cfDirectory.getName().equals(Directories.BACKUPS_SUBDIR))
            {
                cfDirectory = cfDirectory.getParentFile();
            }
            else if (cfDirectory.getParentFile().getName().equals(Directories.SNAPSHOT_SUBDIR))
            {
                cfDirectory = cfDirectory.getParentFile().getParentFile();
            }
            cfname = cfDirectory.getName().split("-")[0] + indexName;
            ksname = cfDirectory.getParentFile().getName();
        }
        else
        {
            cfname = tokenStack.pop();
            ksname = tokenStack.pop();
        }
        assert tokenStack.isEmpty() : "Invalid file name " + name + " in " + directory;

        return Pair.create(new Descriptor(version, parentDirectory, ksname, cfname, generation, type, fmt), component);
    }

    /**
     * @param type temporary flag
     * @return A clone of this descriptor with the given 'temporary' status.
     */
    public Descriptor asType(Type type)
    {
        return new Descriptor(version, directory, ksname, cfname, generation, type, formatType);
    }

    public IMetadataSerializer getMetadataSerializer()
    {
        if (version.hasNewStatsFile())
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
                       && that.formatType == this.formatType
                       && that.type == this.type;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
