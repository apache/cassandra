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
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.IMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
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
    private static final Pattern FORWARD_SLASH_PATTERN = Pattern.compile("/");
    private static final Pattern INDEX_DIR_PATTERN = Pattern.compile("\\.");

    public static String TMP_EXT = ".tmp";

    private static final Splitter filenameSplitter = Splitter.on('-');

    /** canonicalized path to the directory where SSTable resides */
    public final File directory;
    /** version has the following format: <code>[a-z]+</code> */
    public final Version version;
    public final String ksname;
    public final String cfname;
    public final int generation;
    public final SSTableFormat.Type formatType;
    private final int hashCode;

    /**
     * A descriptor that assumes CURRENT_VERSION.
     */
    @VisibleForTesting
    public Descriptor(File directory, String ksname, String cfname, int generation)
    {
        this(SSTableFormat.Type.current().info.getLatestVersion(), directory, ksname, cfname, generation, SSTableFormat.Type.current());
    }

    /**
     * Constructor for sstable writers only.
     */
    public Descriptor(File directory, String ksname, String cfname, int generation, SSTableFormat.Type formatType)
    {
        this(formatType.info.getLatestVersion(), directory, ksname, cfname, generation, formatType);
    }

    public Descriptor(Version version, File directory, String ksname, String cfname, int generation, SSTableFormat.Type formatType)
    {
        assert version != null && directory != null && ksname != null && cfname != null && formatType.info.getLatestVersion().getClass().equals(version.getClass());
        this.version = version;
        try
        {
            this.directory = directory.getCanonicalFile();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        this.ksname = ksname;
        this.cfname = cfname;
        this.generation = generation;
        this.formatType = formatType;

        hashCode = Objects.hashCode(version, this.directory, generation, ksname, cfname, formatType);
    }

    public Descriptor withGeneration(int newGeneration)
    {
        return new Descriptor(version, directory, ksname, cfname, newGeneration, formatType);
    }

    public Descriptor withFormatType(SSTableFormat.Type newType)
    {
        return new Descriptor(newType.info.getLatestVersion(), directory, ksname, cfname, generation, newType);
    }

    public String tmpFilenameFor(Component component)
    {
        return filenameFor(component) + TMP_EXT;
    }

    /**
     * @return a unique temporary file name for given component during entire-sstable-streaming.
     */
    public String tmpFilenameForStreaming(Component component)
    {
        // Use UUID to handle concurrent streamings on the same sstable.
        // TMP_EXT allows temp file to be removed by {@link ColumnFamilyStore#scrubDataDirectories}
        return String.format("%s.%s%s", filenameFor(component), UUIDGen.getTimeUUID(), TMP_EXT);
    }

    public String filenameFor(Component component)
    {
        return baseFilename() + separator + component.name();
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
        buff.append(version).append(separator);
        buff.append(generation);
        buff.append(separator).append(formatType.name);
    }

    public String relativeFilenameFor(Component component)
    {
        final StringBuilder buff = new StringBuilder();
        if (Directories.isSecondaryIndexFolder(directory))
        {
            buff.append(directory.getName()).append(File.separator);
        }

        appendFileName(buff);
        buff.append(separator).append(component.name());
        return buff.toString();
    }

    public SSTableFormat getFormat()
    {
        return formatType.info;
    }

    /** Return any temporary files found in the directory */
    public List<File> getTemporaryFiles()
    {
        File[] tmpFiles = directory.listFiles((dir, name) ->
                                              name.endsWith(Descriptor.TMP_EXT));

        List<File> ret = new ArrayList<>(tmpFiles.length);
        for (File tmpFile : tmpFiles)
            ret.add(tmpFile);

        return ret;
    }

    public static boolean isValidFile(File file)
    {
        String filename = file.getName();
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
            file = file.getAbsoluteFile();

        String name = file.getName();
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

        String versionString = tokens.get(0);
        if (!Version.validate(versionString))
            throw invalidSSTable(name, "invalid version %s", versionString);

        int generation;
        try
        {
            generation = Integer.parseInt(tokens.get(1));
        }
        catch (NumberFormatException e)
        {
            throw invalidSSTable(name, "the 'generation' part of the name doesn't parse as a number");
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

        final File directory = parentOf(name, file);

        final Pair<String, String> keyspaceAndTable = parseKeyspaceAndTable(file);

        return Pair.create(new Descriptor(version, directory, keyspaceAndTable.left, keyspaceAndTable.right, generation, format), component);
    }

    public static Pair<String, String> parseKeyspaceAndTable(final File file)
    {
        return parseKeyspaceAndTable(DatabaseDescriptor.getAllDataFileLocations(), file);
    }

    public static Pair<String, String> parseKeyspaceAndTable(final String[] dataLocations, final File file)
    {
        final String filePath = file.getAbsolutePath();

        final Optional<String> dataDir = Stream.of(dataLocations)
                                               .sorted(Comparator.comparingInt(String::length).reversed())
                                               .filter(path -> {
                                                   String dataLocation = path.endsWith(File.separator) ?  path : path + File.separator;
                                                   return filePath.startsWith(dataLocation);
                                               }).findFirst();

        final Function<String, String> enrichWithSeparator = (pattern) ->
        {
            assert pattern != null;

            if (File.separator.equals("\\"))
            {
                return FORWARD_SLASH_PATTERN.matcher(pattern).replaceAll("\\\\");
            }

            return pattern;
        };

        final Function<String, String> parseTableName = (tableDir) ->
        {
            assert tableDir != null;

            if (!tableDir.contains("-"))
            {
                return tableDir;
            }
            else
            {
                return tableDir.split("-")[0];
            }
        };

        final Supplier<Pattern[]> patternsFunction = () ->
        {
            final String prefix = dataDir.orElse("(.*)");

            return new Pattern[] {

                // /some/path/ks/tab/snapshots/snapshot-name/.index/na-1-big-Index.db
                Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/snapshots/(.*)/(\\..*)/(.*)")),
                // /some/path/ks/tab/snapshots/snapshot-name/na-1-big-Index.db
                Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/snapshots/(.*)/(.*)")),

                // /some/path/ks/tab/backups/.index/na-1-big-Index.db
                Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/backups/(\\..*)/(.*)")),
                // /some/path/ks/tab/backups/na-1-big-Index.db
                Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/backups/(.*)")),

                // /some/path/ks/tab/.index/na-1-big-Index.db
                Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/(\\..*)/(.*)")),
                // /some/path/ks/tab/na-1-big-Index.db
                Pattern.compile(enrichWithSeparator.apply(prefix + "/(.*)/(.*)/(.*)"))
            };
        };

        final Pattern[] patterns = patternsFunction.get();

        int keyspaceGroup = dataDir.isPresent() ? 1 : 2;
        int tableGroup = dataDir.isPresent() ? 2 : 3;
        int indexGroup = dataDir.isPresent() ? 3 : 4;
        int indexGroupInSnapshots = dataDir.isPresent() ? 4 : 5;
        int indexGroupInBackups = dataDir.isPresent() ? 3 : 4;

        final Matcher indexFileInSnapshotMacher = patterns[0].matcher(filePath);
        if (indexFileInSnapshotMacher.matches()) {
            return Pair.create(indexFileInSnapshotMacher.group(keyspaceGroup),
                               parseTableName.apply(indexFileInSnapshotMacher.group(tableGroup)) + indexFileInSnapshotMacher.group(indexGroupInSnapshots));
        }

        final Matcher snapshotFileMatcher = patterns[1].matcher(filePath);
        if (snapshotFileMatcher.matches()) {
            return Pair.create(snapshotFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(snapshotFileMatcher.group(tableGroup)));
        }

        final Matcher indexFileInBackupMatcher = patterns[2].matcher(filePath);
        if (indexFileInBackupMatcher.matches()) {
            return Pair.create(indexFileInBackupMatcher.group(keyspaceGroup),
                               parseTableName.apply(indexFileInBackupMatcher.group(tableGroup)) + indexFileInBackupMatcher.group(indexGroupInBackups));
        }

        final Matcher backupFileMatcher = patterns[3].matcher(filePath);
        if (backupFileMatcher.matches()) {
            return Pair.create(backupFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(backupFileMatcher.group(tableGroup)));
        }

        final Matcher indexFileMatcher = patterns[4].matcher(filePath);
        if (indexFileMatcher.matches()) {
            return Pair.create(indexFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(indexFileMatcher.group(tableGroup)) + indexFileMatcher.group(indexGroup));
        }

        final Matcher normalFileMatcher = patterns[5].matcher(filePath);
        if (normalFileMatcher.matches()) {
            return Pair.create(normalFileMatcher.group(keyspaceGroup),
                               parseTableName.apply(normalFileMatcher.group(tableGroup)));
        }

        throw new IllegalStateException("Unable to parse keyspace and table from " + filePath);
    }

    private static File parentOf(String name, File file)
    {
        File parent = file.getParentFile();
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
                       && that.formatType == this.formatType;
    }

    @Override
    public int hashCode()
    {
        return hashCode;
    }
}
