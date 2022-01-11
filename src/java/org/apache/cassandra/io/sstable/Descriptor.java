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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.IMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.io.storage.StorageProvider;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

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
    private final static String LEGACY_TMP_REGEX_STR = "^((.*)\\-(.*)\\-)?tmp(link)?\\-((?:l|k).)\\-(\\d)*\\-(.*)$";
    private final static Pattern LEGACY_TMP_REGEX = Pattern.compile(LEGACY_TMP_REGEX_STR);

    public static String TMP_EXT = ".tmp";

    private static final Splitter filenameSplitter = Splitter.on('-');

    /** canonicalized path to the directory where SSTable resides */
    public final File directory;
    /** version has the following format: <code>[a-z]+</code> */
    public final Version version;
    public final String ksname;
    public final String cfname;
    public final SSTableUniqueIdentifier generation;
    public final SSTableFormat.Type formatType;
    private final int hashCode;

    private final String baseFileURI;
    private final String filenamePart;

    /**
     * A descriptor that assumes CURRENT_VERSION.
     */
    @VisibleForTesting
    public Descriptor(File directory, String ksname, String cfname, SSTableUniqueIdentifier generation)
    {
        this(SSTableFormat.Type.current().info.getLatestVersion(), directory, ksname, cfname, generation, SSTableFormat.Type.current());
    }

    /**
     * Constructor for sstable writers only.
     */
    public Descriptor(File directory, String ksname, String cfname, SSTableUniqueIdentifier generation, SSTableFormat.Type formatType)
    {
        this(formatType.info.getLatestVersion(), directory, ksname, cfname, generation, formatType);
    }

    @VisibleForTesting
    public Descriptor(String version, File directory, String ksname, String cfname, SSTableUniqueIdentifier generation, SSTableFormat.Type formatType)
    {
        this(formatType.info.getVersion(version), directory, ksname, cfname, generation, formatType);
    }

    public Descriptor(Version version, File directory, String ksname, String cfname, SSTableUniqueIdentifier generation, SSTableFormat.Type formatType)
    {
        assert version != null && directory != null && ksname != null && cfname != null && formatType.info.getLatestVersion().getClass().equals(version.getClass());
        this.version = version;
        this.directory = directory.toCanonical();
        this.ksname = ksname;
        this.cfname = cfname;
        this.generation = generation;
        this.formatType = formatType;

        hashCode = Objects.hashCode(version, this.directory, generation, ksname, cfname, formatType);

        filenamePart = version.toString() + separator + generation + separator + formatType.name;
        String locationURI = directory.toUri().toString();
        if (!locationURI.endsWith(java.io.File.separator))
            locationURI = locationURI + java.io.File.separatorChar;
        baseFileURI = locationURI + filenamePart;
    }

    public Descriptor withGeneration(SSTableUniqueIdentifier newGeneration)
    {
        return new Descriptor(version, directory, ksname, cfname, newGeneration, formatType);
    }

    public Descriptor withFormatType(SSTableFormat.Type newType)
    {
        return new Descriptor(newType.info.getLatestVersion(), directory, ksname, cfname, generation, newType);
    }

    public File tmpFileFor(Component component)
    {
        File file = StorageProvider.instance.getLocalPath(fileFor(component));
        return file.resolveSibling(file.name() + TMP_EXT);
    }

    /**
     * @return a unique temporary file name for given component during entire-sstable-streaming.
     */
    public String tmpFilenameForStreaming(Component component)
    {
        // Use UUID to handle concurrent streamings on the same sstable.
        // TMP_EXT allows temp file to be removed by {@link ColumnFamilyStore#scrubDataDirectories}
        return String.format("%s.%s%s", fileFor(component), UUIDGen.getTimeUUID(), TMP_EXT);
    }

    public File fileFor(Component component)
    {
        return component.getFile(baseFileURI);
    }

    public String baseFileUri()
    {
        return baseFileURI;
    }

    public String relativeFilenameFor(Component component)
    {
        final StringBuilder buff = new StringBuilder();
        if (Directories.isSecondaryIndexFolder(directory))
        {
            buff.append(directory.name()).append(File.pathSeparator());
        }

        return buff.append(filenamePart).append(separator).append(component.name()).toString();
    }

    public SSTableFormat getFormat()
    {
        return formatType.info;
    }

    /** Return any temporary files found in the directory */
    public List<File> getTemporaryFiles()
    {
        File[] tmpFiles = directory.tryList((dir, name) ->
                                              name.endsWith(Descriptor.TMP_EXT));

        List<File> ret = new ArrayList<>(tmpFiles.length);
        for (File tmpFile : tmpFiles)
            ret.add(tmpFile);

        return ret;
    }

    public static boolean isValidFile(File file)
    {
        String filename = file.name();
        return filename.endsWith(".db") && !LEGACY_TMP_REGEX.matcher(filename).matches();
    }

    /**
     * Parse a sstable filename into a Descriptor.
     * <p>
     * This is a shortcut for {@code fromFilename(new File(filename))}.
     *
     * @param filename the filename to a sstable component.
     * @return the descriptor for the parsed file.
     *
     * @throws IllegalArgumentException if the provided {@code file} does point to a valid sstable filename. This could
     * mean either that the filename doesn't look like a sstable file, or that it is for an old and unsupported
     * versions.
     */
    public static Descriptor fromFilename(String filename)
    {
        return fromFilename(new File(filename));
    }

    /**
     * Parse a sstable filename into a Descriptor.
     * <p>
     * SSTables files are all located within subdirectories of the form {@code <keyspace>/<table>/}. Normal sstables are
     * are directly within that subdirectory structure while 2ndary index, backups and snapshot are each inside an
     * additional subdirectory. The file themselves have the form:
     *   {@code <version>-<gen>-<format>-<component>}.
     * <p>
     * Note that this method will only sucessfully parse sstable files of supported versions.
     *
     * @param file the {@code File} object for the filename to parse.
     * @return the descriptor for the parsed file.
     *
     * @throws IllegalArgumentException if the provided {@code file} does point to a valid sstable filename. This could
     * mean either that the filename doesn't look like a sstable file, or that it is for an old and unsupported
     * versions.
     */
    public static Descriptor fromFilename(File file)
    {
        return fromFilenameWithComponent(file).left;
    }

    /**
     * Parse a sstable filename, extracting both the {@code Descriptor} and {@code Component} part.
     *
     * @param file the {@code File} object for the filename to parse.
     * @return a pair of the descriptor and component corresponding to the provided {@code file}.
     *
     * @throws IllegalArgumentException if the provided {@code file} does point to a valid sstable filename. This could
     * mean either that the filename doesn't look like a sstable file, or that it is for an old and unsupported
     * versions.
     */
    public static Pair<Descriptor, Component> fromFilenameWithComponent(File file)
    {
        // We need to extract the keyspace and table names from the parent directories, so make sure we deal with the
        // absolute path.
        if (!file.isAbsolute())
            file = file.toAbsolute();

        String name = file.name();
        List<String> tokens = filenameTokens(name);

        String versionString = tokens.get(0);
        if (!Version.validate(versionString))
            throw invalidSSTable(name, "invalid version %s", versionString);

        SSTableUniqueIdentifier generation;
        try
        {
            generation = SSTableUniqueIdentifierFactory.instance.fromString(tokens.get(1));
        }
        catch (RuntimeException e)
        {
            throw invalidSSTable(name, "the 'generation' part of the name doesn't parse as a valid unique identifier");
        }

        String formatString = tokens.get(2);
        SSTableFormat.Type format;
        try
        {
            format = SSTableFormat.Type.validate(formatString);
        }
        catch (IllegalArgumentException e)
        {
            throw invalidSSTable(name, "unknown 'format' part (%s)", formatString);
        }

        Component component = Component.parse(tokens.get(3));

        Version version = format.info.getVersion(versionString);
        if (!version.isCompatible())
            throw invalidSSTable(name, "incompatible sstable version (%s); you should have run upgradesstables before upgrading", versionString);

        File directory = parentOf(name, file);
        File tableDir = directory;

        // Check if it's a 2ndary index directory (not that it doesn't exclude it to be also a backup or snapshot)
        String indexName = "";
        if (Directories.isSecondaryIndexFolder(tableDir))
        {
            indexName = tableDir.name();
            tableDir = parentOf(name, tableDir);
        }

        // Then it can be a backup or a snapshot
        if (tableDir.name().equals(Directories.BACKUPS_SUBDIR))
            tableDir = tableDir.parent();
        else if (parentOf(name, tableDir).name().equals(Directories.SNAPSHOT_SUBDIR))
            tableDir = parentOf(name, parentOf(name, tableDir));

        String table = tableDir.name().split("-")[0] + indexName;
        String keyspace = parentOf(name, tableDir).name();

        return Pair.create(new Descriptor(version, directory, keyspace, table, generation, format), component);
    }

    public static Component validFilenameWithComponent(String name)
    {
        try
        {
            List<String> tokens = filenameTokens(name);

            String versionString = tokens.get(0);
            if (!Version.validate(versionString))
                return null;

            SSTableUniqueIdentifierFactory.instance.fromString(tokens.get(1));

            String formatString = tokens.get(2);
            SSTableFormat.Type.validate(formatString);

            return Component.parse(tokens.get(3));
        }
        catch (Exception e)
        {
            return null;
        }
    }

    public static boolean validFilename(String name)
    {
        return validFilenameWithComponent(name) != null;
    }

    private static List<String> filenameTokens(String name)
    {
        List<String> tokens = filenameSplitter.splitToList(name);
        int size = tokens.size();

        if (size != 4)
        {
            // This is an invalid sstable file for this version. But to provide a more helpful error message, we detect
            // old format sstable, which had the format:
            //   <keyspace>-<table>-(tmp-)?<version>-<gen>-<component>
            // Note that we assume it's an old format sstable if it has the right number of tokens: this is not perfect
            // but we're just trying to be helpful, not perfect.
            if (size == 5 || size == 6)
                throw new IllegalArgumentException(String.format("%s is of version %s which is now unsupported and cannot be read.",
                                                                 name,
                                                                 tokens.get(size - 3)));
            throw new IllegalArgumentException(String.format("Invalid sstable file %s: the name doesn't look like a supported sstable file name", name));
        }
        return tokens;
    }
    
    private static File parentOf(String name, File file)
    {
        File parent = file.parent();
        if (parent == null)
            throw invalidSSTable(name, "cannot extract keyspace and table name; make sure the sstable is in the proper sub-directories");
        return parent;
    }

    private static IllegalArgumentException invalidSSTable(String name, String msgFormat, Object... parameters)
    {
        throw new IllegalArgumentException(String.format("Invalid sstable file " + name + ": " + msgFormat, parameters));
    }

    public IMetadataSerializer getMetadataSerializer()
    {
        return new MetadataSerializer();
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
        return baseFileUri();
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
                       && that.generation.equals(this.generation)
                       && that.ksname.equals(this.ksname)
                       && that.cfname.equals(this.cfname)
                       && that.version.equals(this.version)
                       && that.formatType == this.formatType;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
