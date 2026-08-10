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
package org.apache.cassandra.index.sai.disk.format;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v3.V3OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v4.V4OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v5.V5OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v6.V6OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v7.V7OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v8.V8OnDiskFormat;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Format version of indexing component, denoted as [major][minor]. Same forward-compatibility rules apply as to
 * {@link org.apache.cassandra.io.sstable.format.Version}.
 */
public class Version implements Comparable<Version>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Version.class);
    // 6.8 formats
    public static final Version AA = new Version("aa", V1OnDiskFormat.instance, Version::aaFileNameFormat);
    // Stargazer
    public static final Version BA = new Version("ba", V2OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "ba"));
    // Converged Cassandra with JVector with file format version 2
    // Note: vector index checksums for TERMS files were computed in two different ways for this version. As such,
    // we do not validate checksums for this version or any subsequent version until EC.
    public static final Version CA = new Version("ca", V3OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "ca"));
    // NOTE: use DB to prevent collisions with upstream file formats
    // Encode trie entries using their AbstractType to ensure trie entries are sorted for range queries and are prefix free.
    public static final Version DB = new Version("db", V4OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "db"));
    // revamps vector postings lists to cause fewer reads from disk
    public static final Version DC = new Version("dc", V5OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "dc"));
    // histograms in index metadata
    public static final Version EB = new Version("eb", V6OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "eb"));
    // term frequencies index component (support for BM25); bump jvector file format version to 4
    // Start validating vector index component checksums, except for the TERMS_FILE because it's checksum is non-standard
    // and isn't easily validated when an sstable index has multiple segments within the TERMS_FILE.
    public static final Version EC = new Version("ec", V7OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "ec"));
    // total terms count serialization in index metadata, enables ANN_USE_SYNTHETIC_SCORE by default
    public static final Version ED = new Version("ed", V7OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "ed"));
    // jvector file format version 6 (skipped 5). FusedPQ is always enabled regardless of cassandra.sai.vector.enable_fused. cassandra.sai.vector.enable_fused is ignored
    public static final Version FA = new Version("fa", V8OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "fa"));
    // FB format: same jvector file format version 6 as FA, but FusedPQ is opt-in via cassandra.sai.vector.enable_fused
    public static final Version FB = new Version("fb", V8OnDiskFormat.instance, (c, i, g) -> stargazerFileNameFormat(c, i, g, "fb"));

    // These are in reverse-chronological order so that the latest version is first. Version matching tests
    // are more likely to match the latest version, so we want to test that one first.
    public static final List<Version> ALL = Lists.newArrayList(FB, FA, ED, EC, EB, DC, DB, CA, BA, AA);

    public static final Version EARLIEST = AA;
    public static final Version VECTOR_EARLIEST = BA;
    public static final Version JVECTOR_EARLIEST = CA;
    public static final Version BM25_EARLIEST = EC;
    public static final Version LATEST = ALL.get(0);

    // This is volatile rather than final so that tests may use reflection to change it and safely publish across threads,
    // but it should not be changed outside of tests.
    @SuppressWarnings("FieldMayBeFinal")
    private static volatile Selector SELECTOR = Selector.fromProperty();

    private static final Pattern GENERATION_PATTERN = Pattern.compile("\\d+");

    private final String version;
    private final OnDiskFormat onDiskFormat;
    private final FileNameFormatter fileNameFormatter;

    private Version(String version, OnDiskFormat onDiskFormat, FileNameFormatter fileNameFormatter)
    {
        this.version = version;
        this.onDiskFormat = onDiskFormat;
        this.fileNameFormatter = fileNameFormatter;
    }

    public static Version parse(String input)
    {
        checkArgument(input != null);
        checkArgument(input.length() == 2);
        for (var v : ALL)
        {
            if (input.equals(v.version))
                return v;
        }
        throw new IllegalArgumentException("Unrecognized SAI version string " + input);
    }

    /**
     * @param keyspace the keyspace for which to select a version, assumed to belong to an existing keyspace.
     * @return the version to use on new SSTables for the provided keyspace.
     */
    public static Version current(String keyspace)
    {
        assert keyspace != null : "Keyspace name must not be null";
        return SELECTOR.select(keyspace);
    }

    /**
     * Calculates the maximum allowed length for SAI index names to ensure generated filenames
     * do not exceed the system's filename length limit (defined in {@link SchemaConstants#FILENAME_LENGTH}).
     * This accounts for all additional components in the filename.
     * It is only used to validate that {@link SchemaConstants#INDEX_NAME_LENGTH}
     * is not bigger than the actual acceptable length.
     */
    @VisibleForTesting
    public static int calculateIndexNameAllowedLength(String keyspace)
    {
        int addedLength = getAddedLengthFromDescriptorAndVersion(keyspace);
        assert addedLength < SchemaConstants.FILENAME_LENGTH;
        return SchemaConstants.FILENAME_LENGTH - addedLength;
    }

    /**
     * Calculates the length of the added prefixes and suffixes from Descriptor constructor
     * and {@link Version#stargazerFileNameFormat}.
     *
     * @return the length of the added prefixes and suffixes
     */
    private static int getAddedLengthFromDescriptorAndVersion(String keyspace)
    {
        // Prefixes and suffixes constructed by Version.stargazerFileNameFormat
        int versionNameLength = current(keyspace).toString().length();
        // room for up to 999 generations
        int generationLength = 3 + SAI_SEPARATOR.length();
        int addedLength = SAI_DESCRIPTOR.length()
                          + versionNameLength
                          + generationLength
                          + calculateMaxComponentRepresentationLength()
                          + SAI_SEPARATOR.length() * 3
                          + EXTENSION.length();

        // Prefixes from Descriptor constructor
        int separatorLength = 1;
        int indexVersionLength = 2;
        int tableIdLength = 28;
        addedLength += indexVersionLength
                       + SSTableFormat.Type.BTI.name().length()
                       + tableIdLength
                       + separatorLength * 3;
        return addedLength;
    }

    private static int calculateMaxComponentRepresentationLength()
    {
        int maxLength = 0;
        for (IndexComponentType component : IndexComponentType.values())
            maxLength = Math.max(maxLength, component.representation.length());
        return maxLength;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Version other = (Version) o;
        return Objects.equal(version, other.version);
    }

    @Override
    public String toString()
    {
        return version;
    }

    // Useful for handling features that need a two phase rollout.
    public boolean after(Version other)
    {
        return version.compareTo(other.version) > 0;
    }

    public boolean onOrAfter(Version other)
    {
        return version.compareTo(other.version) >= 0;
    }

    public OnDiskFormat onDiskFormat()
    {
        return onDiskFormat;
    }

    public FileNameFormatter fileNameFormatter()
    {
        return fileNameFormatter;
    }

    public boolean useImmutableComponentFiles()
    {
        return CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS.getBoolean()
               && onOrAfter(parse(CassandraRelevantProperties.IMMUTABLE_SAI_COMPONENTS_MIN_VERSION.getString()));
    }

    @Override
    public int compareTo(Version other)
    {
        return this.version.compareTo(other.version);
    }

    public interface FileNameFormatter
    {
        /**
         * Format filename for given index component, context and generation.  Only the "component" part of the
         * filename is returned (so the suffix of the full filename), not a full path.
         */
        default String format(IndexComponentType indexComponentType, IndexContext indexContext, int generation)
        {
            return format(indexComponentType, indexContext == null ? null : indexContext.getIndexName(), generation);
        }

        /**
         * Format filename for given index component, index and generation.  Only the "component" part of the
         * filename is returned (so the suffix of the full filename), not a full path.
         *
         * @param indexComponentType the type of the index component.
         * @param indexName          the name of the index, or {@code null} for a per-sstable component.
         * @param generation         the generation of the build of the component.
         */
        String format(IndexComponentType indexComponentType, @Nullable String indexName, int generation);
    }

    /**
     * Try to parse the provided file name as a SAI component file name.
     *
     * @param filename the file name to try to parse.
     * @return the information parsed from the provided file name if it can be successfully parsed, or an empty optional
     * if the file name is not recognized as a SAI component file name for a supported version.
     */
    public static Optional<ParsedFileName> tryParseFileName(String filename)
    {
        if (!filename.endsWith(EXTENSION))
            return Optional.empty();

        // For flexibility, we handle both "full" filename, of the form "<descriptor>-SAI+....db", or just the component
        // part, that is "SAI+....db". In the former, the following `lastIndexOf` will match, and we'll set
        // `startOfComponent` at the beginning of "SAI", and in the later it will not match and return -1, which, with
        // the +1 will also be set at the beginning of "SAI".
        int startOfComponent = filename.lastIndexOf('-') + 1;

        String componentStr = filename.substring(startOfComponent);
        if (componentStr.startsWith("SAI_"))
            return tryParseAAFileName(componentStr);
        else if (componentStr.startsWith("SAI" + SAI_SEPARATOR))
            return tryParseStargazerFileName(componentStr);
        else
            return Optional.empty();
    }

    public static class ParsedFileName
    {
        public final ComponentsBuildId buildId;
        public final IndexComponentType component;
        public final @Nullable String indexName;

        private ParsedFileName(ComponentsBuildId buildId, IndexComponentType component, @Nullable String indexName)
        {
            this.buildId = buildId;
            this.component = component;
            this.indexName = indexName;
        }
    }

    //
    // Version.AA filename formatter. This is the old DSE 6.8 SAI on-disk filename format
    //
    // Format: <sstable descriptor>-SAI(_<index name>)_<component name>.db
    //
    private static final String VERSION_AA_PER_SSTABLE_FORMAT = "SAI_%s.db";
    private static final String VERSION_AA_PER_SSTABLE_WITH_GENERATION_FORMAT = "SAI_%s_%d.db";
    private static final String VERSION_AA_PER_INDEX_FORMAT = "SAI_%s_%s.db";
    private static final String VERSION_AA_PER_INDEX_WITH_GENERATION_FORMAT = "SAI_%s_%s_%d.db";

    private static String aaFileNameFormat(IndexComponentType indexComponentType, @Nullable String indexName, int generation)
    {
        if (generation > 0)
            return (indexName == null ? String.format(VERSION_AA_PER_SSTABLE_WITH_GENERATION_FORMAT, indexComponentType.representation, generation)
                                  : String.format(VERSION_AA_PER_INDEX_WITH_GENERATION_FORMAT, indexName, indexComponentType.representation, generation));

        return (indexName == null ? String.format(VERSION_AA_PER_SSTABLE_FORMAT, indexComponentType.representation)
                                  : String.format(VERSION_AA_PER_INDEX_FORMAT, indexName, indexComponentType.representation));
    }

    private static Optional<ParsedFileName> tryParseAAFileName(String componentStr)
    {
        int lastSepIdx = componentStr.lastIndexOf('_');
        if (lastSepIdx == -1)
            return Optional.empty();

        int generation = 0;
        // This method is only called by `tryParseFileName` which ensures the `componentStr` ends with ".db", so
        // `length() - 3` below is safe.
        assert lastSepIdx + 1 <= componentStr.length() - 3;
        String maybeGenerationStr = componentStr.substring(lastSepIdx + 1, componentStr.length() - 3);
        String indexComponentStr = maybeGenerationStr;
        if (GENERATION_PATTERN.matcher(maybeGenerationStr).matches())
        {
            generation = Integer.parseInt(maybeGenerationStr);
            int prevSepIdx = componentStr.substring(0, lastSepIdx).lastIndexOf('_');
            if (prevSepIdx == -1)
                return Optional.empty();
            indexComponentStr = componentStr.substring(prevSepIdx + 1, lastSepIdx);
            lastSepIdx = prevSepIdx;
        }

        IndexComponentType indexComponentType = IndexComponentType.fromRepresentation(indexComponentStr);
        if (indexComponentType == null)
            return Optional.empty();

        String indexName = null;
        int firstSepIdx = componentStr.indexOf('_');
        if (firstSepIdx != -1 && firstSepIdx != lastSepIdx)
            indexName = componentStr.substring(firstSepIdx + 1, lastSepIdx);

        return Optional.of(new ParsedFileName(ComponentsBuildId.of(AA, generation), indexComponentType, indexName));
    }

    /**
     * This is an interface for selecting the appropriate version of the on-disk format to use.
     * This is going to be used by CNDB to inject the version of SAI to use for a given tenant
     */
    public interface Selector
    {
        /**
         * Default version used by {@link #DEFAULT}.
         */
        Version DEFAULT_VERSION = Version.parse(CassandraRelevantProperties.SAI_CURRENT_VERSION.getString());

        /**
         * Default version selector that uses the same version for all keyspaces.
         */
        Selector DEFAULT = keyspace -> DEFAULT_VERSION;

        /**
         * @param keyspace the keyspace for which to select a version, assumed to belong to an existing keyspace.
         * @return the version of the on-disk format to use for the provided keyspace, it should not be null.
         */
        @Nonnull
        Version select(@Nonnull String keyspace);

        static Selector fromProperty()
        {
            try
            {
                String selectorClass = CassandraRelevantProperties.SAI_VERSION_SELECTOR_CLASS.getString();
                if (selectorClass.isEmpty())
                {
                    return Selector.DEFAULT;
                }
                else
                {
                    LOGGER.info("Using SAI version selector: {}", selectorClass);
                    return FBUtilities.construct(selectorClass, "SAI version selector");
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    //
    // Stargazer filename formatter. This is the current SAI on-disk filename format
    //
    // Format: <sstable descriptor>-SAI+<version>(+<generation>)(+<index name>)+<component name>.db
    //
    public static final String SAI_DESCRIPTOR = "SAI";
    private static final String SAI_SEPARATOR = "+";
    private static final String EXTENSION = ".db";

    private static String stargazerFileNameFormat(IndexComponentType indexComponentType, @Nullable String indexName, int generation, String version)
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(SAI_DESCRIPTOR);
        stringBuilder.append(SAI_SEPARATOR).append(version);
        if (generation > 0)
            stringBuilder.append(SAI_SEPARATOR).append(generation);
        if (indexName != null)
            stringBuilder.append(SAI_SEPARATOR).append(indexName);
        stringBuilder.append(SAI_SEPARATOR).append(indexComponentType.representation);
        stringBuilder.append(EXTENSION);

        return stringBuilder.toString();
    }

    public ByteComparable.Version byteComparableVersionFor(IndexComponentType component, org.apache.cassandra.io.sstable.format.Version sstableFormatVersion)
    {
        return this == AA && component == IndexComponentType.TERMS_DATA
               ? sstableFormatVersion.getByteComparableVersion()
               : TypeUtil.BYTE_COMPARABLE_VERSION;
    }

    private static Optional<ParsedFileName> tryParseStargazerFileName(String componentStr)
    {
        // We skip the beginning `SAI+` and ending `.db` parts.
        String[] splits = componentStr.substring(4, componentStr.length() - 3).split("\\+");
        if (splits.length < 2 || splits.length > 4)
            return Optional.empty();

        Version version = parse(splits[0]);
        IndexComponentType indexComponentType = IndexComponentType.fromRepresentation(splits[splits.length - 1]);

        int generation = 0;
        String indexName = null;
        if (splits.length > 2)
        {
            // If we have 4 parts, then we know we have both the generation and index name. If we have 3
            // however, it means we have either one, but we don't know which, so we check if the additional
            // part is a number or not to distinguish.
            boolean hasGeneration = splits.length == 4 || GENERATION_PATTERN.matcher(splits[1]).matches();
            boolean hasIndexName = splits.length == 4 || !hasGeneration;
            if (hasGeneration)
                generation = Integer.parseInt(splits[1]);
            if (hasIndexName)
                indexName = splits[splits.length - 2];
        }

        return Optional.of(new ParsedFileName(ComponentsBuildId.of(version, generation), indexComponentType, indexName));
    }
}
