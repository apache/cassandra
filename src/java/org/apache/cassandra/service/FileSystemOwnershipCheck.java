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

package org.apache.cassandra.service;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.service.StartupChecks.StartupCheckType.check_filesystem_ownership;

/**
 * Ownership markers on disk are compatible with the java property file format.
 * (https://docs.oracle.com/javase/8/docs/api/java/util/Properties.html#load-java.io.Reader-)
 *
 * This simple formatting is intended to enable them to be created either
 * manually or by automated tooling using minimal standard tools (editor/shell
 * builtins/etc).
 * The only mandatory property is version, which must be parseable as an int
 * and upon which the futher set of required properties will depend.
 *
 * In version 1, two further property values are required:
 * - volume_count
 *   to be parsed as an int representing the number of mounted volumes where
 *   a marker file is expected.
 * - ownership_token
 *   must contain a non-empty token string that can be compared to one
 *   derived from system properties. For version 1, this is simply the cluster name.
 *
 * For this check to succeed as of version 1 then:
 * - There must be a single properties file found in the fs tree for each
 *   target directory.
 * - Every file found must contain the mandatory version property with the
 *   literal value '1'.
 * - The value of the ownership_token property in each file must match the
 *   cluster name
 * - The value of the volume_count property must be an int which must matches
 *   the number of distinct marker files found when traversing the filesystem.
 *
 * In overridden implementations, you will need to override {@link #constructTokenFromProperties(Map)}
 * and add the related *_PROPERTY values you will want the system to check on startup to confirm ownership.
 */
public class FileSystemOwnershipCheck implements StartupCheck
{
    private static final Logger logger = LoggerFactory.getLogger(FileSystemOwnershipCheck.class);

    public static final String FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN = "CassandraOwnershipToken";
    public static final String DEFAULT_FS_OWNERSHIP_FILENAME = ".cassandra_fs_ownership";

    // Ownership file properties
    static final String VERSION                                 = "version";
    static final String VOLUME_COUNT                            = "volume_count";
    static final String TOKEN                                   = "ownership_token";

    // Error strings
    static final String ERROR_PREFIX                            = "FS ownership check failed; ";
    static final String MISSING_PROPERTY                        = "property '%s' required for fs ownership check not supplied";
    static final String NO_OWNERSHIP_FILE                       = "no file found in tree for %s";
    static final String MULTIPLE_OWNERSHIP_FILES                = "multiple files found in tree for %s";
    static final String INCONSISTENT_FILES_FOUND                = "inconsistent ownership files found on disk: %s";
    static final String INVALID_FILE_COUNT                      = "number of ownership files found doesn't match expected";
    static final String MISMATCHING_TOKEN                       = "token found on disk does not match supplied";
    static final String UNSUPPORTED_VERSION                     = "unsupported version '%s' in ownership file";
    static final String INVALID_PROPERTY_VALUE                  = "invalid or missing value for property '%s'";
    static final String READ_EXCEPTION                          = "error when checking for fs ownership file";

    private final Supplier<Iterable<String>> dirs;

    FileSystemOwnershipCheck()
    {
        this(() -> Iterables.concat(Arrays.asList(DatabaseDescriptor.getAllDataFileLocations()),
                                    Arrays.asList(DatabaseDescriptor.getCommitLogLocation(),
                                                  DatabaseDescriptor.getSavedCachesLocation(),
                                                  DatabaseDescriptor.getHintsDirectory().absolutePath())));
    }

    @VisibleForTesting
    FileSystemOwnershipCheck(Supplier<Iterable<String>> dirs)
    {
        this.dirs = dirs;
    }

    @Override
    public StartupChecks.StartupCheckType getStartupCheckType()
    {
        return check_filesystem_ownership;
    }

    @Override
    public void execute(StartupChecksOptions options) throws StartupException
    {
        if (!isEnabled(options))
        {
            logger.info("Filesystem ownership check is not enabled.");
            return;
        }

        Map<String, Object> config = options.getConfig(getStartupCheckType());

        String expectedToken = constructTokenFromProperties(config);
        String tokenFilename = getFsOwnershipFilename(config);
        Map<String, Integer> foundPerTargetDir = new HashMap<>();
        Map<Path, Properties> foundProperties = new HashMap<>();

        // Step 1: Traverse the filesystem from each target dir upward, looking for marker files
        for (String dataDir : dirs.get())
        {
            logger.info("Checking for fs ownership details in file hierarchy for {}", dataDir);
            int foundFiles = 0;
            Path dir = File.getPath(dataDir).normalize();
            do
            {
                File tokenFile = resolve(dir, tokenFilename);
                if (tokenFile.exists())
                {
                    foundFiles++;
                    if (!foundProperties.containsKey(tokenFile.toPath().toAbsolutePath()))
                    {
                        try (BufferedReader reader = Files.newBufferedReader(tokenFile.toPath()))
                        {
                            Properties props = new Properties();
                            props.load(reader);
                            foundProperties.put(tokenFile.toPath().toAbsolutePath(), props);
                        }
                        catch (Exception e)
                        {
                            logger.error("Error reading fs ownership file from disk", e);
                            throw exception(READ_EXCEPTION);
                        }
                    }
                }
                dir = dir.getParent();
            } while (dir != null);

            foundPerTargetDir.put(dataDir, foundFiles);
        }

        // If a marker file couldn't be found for every target directory, error.
        if (foundPerTargetDir.containsValue(0))
        {
            throw exception(String.format(NO_OWNERSHIP_FILE, foundPerTargetDir.entrySet()
                                                                              .stream()
                                                                              .filter(e -> e.getValue() == 0)
                                                                              .map(Map.Entry::getKey)
                                                                              .collect(Collectors.joining("', '", "'", "'"))));
        }

        // If more than one marker file was found in the tree for any target directory, error
        Set<String> multipleTokens = foundPerTargetDir.entrySet()
                                                      .stream()
                                                      .filter(e -> e.getValue() > 1)
                                                      .map(Map.Entry::getKey)
                                                      .collect(Collectors.toSet());
        if (!multipleTokens.isEmpty())
            throw exception(String.format(MULTIPLE_OWNERSHIP_FILES, String.join(",", multipleTokens)));

        // Step 2: assert that the content of each file is identical
        assert !foundProperties.isEmpty();
        Multimap<Integer, Path> byHash = HashMultimap.create();
        foundProperties.forEach((key, value) -> byHash.put(value.hashCode(), key));
        if (byHash.keySet().size() > 1)
        {
            // Group identical files to highlight where the mismatches are, worst case is
            // they're all unique, but even then the number of individual files should be low
            throw exception(String.format(INCONSISTENT_FILES_FOUND,
                                          byHash.keySet()
                                                .stream()
                                                .map(hash -> byHash.get(hash)
                                                                   .stream()
                                                                   .map(Path::toString)
                                                                   .sorted()
                                                                   .collect(Collectors.joining("', '", "['", "']")))
                                                .sorted()
                                                .collect(Collectors.joining(", "))));
        }

        // Step 3: validate the content of the properties from disk
        // Currently, only version 1 is supported which requires:
        //   volume_count       that matches the number of unique files we found
        //   ownership_token    that matches the one constructed from system props
        Properties fromDisk = foundProperties.entrySet().iterator().next().getValue();
        int version = getIntProperty(fromDisk, VERSION);
        if (version != 1)
            throw exception(String.format(UNSUPPORTED_VERSION, version));

        int volumeCount = getIntProperty(fromDisk, VOLUME_COUNT);
        if (volumeCount != foundProperties.size())
            throw exception(INVALID_FILE_COUNT);

        String token = getRequiredProperty(fromDisk, TOKEN);
        if (!expectedToken.equals(token))
            throw exception(MISMATCHING_TOKEN);

        logger.info("Successfully verified fs ownership");
    }

    /** In version 1, we check and return the ownership token. Extend this for custom ownership hierarchies. */
    protected String constructTokenFromProperties(Map<String, Object> config) throws StartupException
    {
        String cluster = getOwnershipToken(config);
        if (null == cluster || cluster.isEmpty())
            throw exception(String.format(MISSING_PROPERTY, FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN));
        return cluster;
    }

    private int getIntProperty(Properties props, String key) throws StartupException
    {
        String val = getRequiredProperty(props, key);
        try
        {
            return Integer.parseInt(val);
        }
        catch (NumberFormatException e)
        {
            throw exception(String.format(INVALID_PROPERTY_VALUE, key));
        }
    }

    private String getRequiredProperty(Properties props, String key) throws StartupException
    {
        String s = props.getProperty(key);
        if (null == s || s.isEmpty())
            throw exception(String.format(INVALID_PROPERTY_VALUE, key));
        return s;
    }

    private File resolve(Path dir, String filename) throws StartupException
    {
        try
        {
            return new File(dir.resolve(filename));
        }
        catch (Exception e)
        {
            logger.error("Encountered error resolving path ownership file {} relative to dir {}", filename, dir);
            throw exception(READ_EXCEPTION);
        }
    }

    private StartupException exception(String message)
    {
        return new StartupException(StartupException.ERR_WRONG_DISK_STATE, ERROR_PREFIX + message);
    }

    public boolean isEnabled(StartupChecksOptions options)
    {
        boolean enabledFromYaml = options.isEnabled(getStartupCheckType());
        return CassandraRelevantProperties.FILE_SYSTEM_CHECK_ENABLE.getBoolean(enabledFromYaml);
    }

    public String getFsOwnershipFilename(Map<String, Object> config)
    {
        if (CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME.isPresent())
        {
            logger.warn(String.format("Cassandra system property flag %s is deprecated and you should " +
                                      "use startup check configuration in cassandra.yaml",
                                      CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME.getKey()));
            return CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME.getString();
        }
        else
        {
            Object fsOwnershipFilename = config.get("ownership_filename");
            return fsOwnershipFilename == null
                   ? CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME.getDefaultValue()
                   : (String) fsOwnershipFilename;
        }
    }

    public String getOwnershipToken(Map<String, Object> config)
    {
        if (CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.isPresent())
        {
            logger.warn(String.format("Cassandra system property flag %s is deprecated and you should " +
                                      "use startup check configuration in cassandra.yaml",
                                      CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.getKey()));
            return CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.getString();
        }
        else
        {
            Object ownershipToken = config.get("ownership_token");
            return ownershipToken == null
                   ? CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.getDefaultValue()
                   : (String) ownershipToken;
        }
    }
}