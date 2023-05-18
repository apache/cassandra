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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.StartupChecksOptions;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.StartupException;
import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.service.FileSystemOwnershipCheck.DEFAULT_FS_OWNERSHIP_FILENAME;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.ERROR_PREFIX;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.INCONSISTENT_FILES_FOUND;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.INVALID_FILE_COUNT;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.INVALID_PROPERTY_VALUE;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.MISMATCHING_TOKEN;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.MISSING_PROPERTY;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.MULTIPLE_OWNERSHIP_FILES;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.NO_OWNERSHIP_FILE;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.READ_EXCEPTION;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.TOKEN;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.UNSUPPORTED_VERSION;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.VERSION;
import static org.apache.cassandra.service.FileSystemOwnershipCheck.VOLUME_COUNT;
import static org.apache.cassandra.service.StartupChecks.StartupCheckType.check_filesystem_ownership;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public abstract class AbstractFilesystemOwnershipCheckTest
{
    protected File tempDir;
    protected String token;

    protected StartupChecksOptions options = new StartupChecksOptions();

    static WithProperties properties;

    protected void setup()
    {
        cleanTempDir();
        tempDir = new File(com.google.common.io.Files.createTempDir());
        token = makeRandomString(10);
        properties = new WithProperties();
        System.clearProperty(CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME.getKey());
        System.clearProperty(CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.getKey());
        System.clearProperty(CassandraRelevantProperties.FILE_SYSTEM_CHECK_ENABLE.getKey());
    }

    static File writeFile(File dir, String filename, Properties props) throws IOException
    {
        File tokenFile = new File(dir, filename); //checkstyle: permit this instantiation
        assertTrue(tokenFile.createFileIfNotExists());
        try (OutputStream os = Files.newOutputStream(tokenFile.toPath()))
        {
            props.store(os, "Test properties");
        }
        assertTrue(tokenFile.isReadable());
        return tokenFile;
    }

    private static void executeAndFail(FileSystemOwnershipCheck checker,
                                       StartupChecksOptions options,
                                       String messageTemplate,
                                       Object...messageArgs)
    {
        try
        {
            checker.execute(options);
            fail("Expected an exception but none thrown");
        } catch (StartupException e) {
            String expected = ERROR_PREFIX + String.format(messageTemplate, messageArgs);
            assertEquals(expected, e.getMessage());
        }
    }

    private static Properties makeProperties(int version, int volumeCount, String token)
    {
        Properties props = new Properties();
        props.setProperty(VERSION, Integer.toString(version));
        props.setProperty(VOLUME_COUNT, Integer.toString(volumeCount));
        props.setProperty(TOKEN, token);
        return props;
    }

    private static File writeFile(File dir, int volumeCount, String token) throws IOException
    {
        return AbstractFilesystemOwnershipCheckTest.writeFile(dir, DEFAULT_FS_OWNERSHIP_FILENAME, 1, volumeCount, token);
    }

    private static File writeFile(File dir, final String filename, int version, int volumeCount, String token)
    throws IOException
    {
        return writeFile(dir, filename, AbstractFilesystemOwnershipCheckTest.makeProperties(version, volumeCount, token));
    }

    private static File mkdirs(File parent, String path)
    {
        File childDir = new File(parent, path); //checkstyle: permit this instantiation
        assertTrue(childDir.tryCreateDirectories());
        assertTrue(childDir.exists());
        return childDir;
    }

    private static FileSystemOwnershipCheck checker(Supplier<Iterable<String>> dirs)
    {
        return new FileSystemOwnershipCheck(dirs);
    }

    private static FileSystemOwnershipCheck checker(File...dirs)
    {
        return checker(() -> Arrays.stream(dirs).map(File::absolutePath).collect(Collectors.toList()));
    }

    private static FileSystemOwnershipCheck checker(String...dirs)
    {
        return checker(() -> Arrays.asList(dirs));
    }

    public static String makeRandomString(int length)
    {
        Random random = new Random();
        char[] chars = new char[length];
        for (int i = 0; i < length; ++i)
            chars[i] = (char) ('a' + random.nextInt('z' - 'a' + 1));
        return new String(chars);
    }

    protected void cleanTempDir()
    {
        if (tempDir != null && tempDir.exists())
            delete(tempDir);
    }

    private void delete(File file)
    {
        file.trySetReadable(true);
        file.trySetWritable(true);
        file.trySetExecutable(true);
        File[] files = file.tryList();
        if (files != null)
        {
            for (File child : files)
            {
                delete(child);
            }
        }
        file.delete();
    }

    @BeforeClass
    public static void setupConfig()
    {
        // PathUtils touches StorageService which touches StreamManager which requires configs be setup
        DatabaseDescriptor.daemonInitialization();
    }

    @After
    public void teardown() throws IOException
    {
        cleanTempDir();
        properties.close();
    }

    // tests for enabling/disabling/configuring the check
    @Test
    public void skipCheckDisabledIfSystemPropertyIsEmpty() throws Exception
    {
        // no exceptions thrown from the supplier because the check is skipped
        options.disable(check_filesystem_ownership);
        System.clearProperty(CassandraRelevantProperties.FILE_SYSTEM_CHECK_ENABLE.getKey());
        AbstractFilesystemOwnershipCheckTest.checker(() -> { throw new RuntimeException("FAIL"); }).execute(options);
    }

    @Test
    public void skipCheckDisabledIfSystemPropertyIsFalseButOptionsEnabled() throws Exception
    {
        // no exceptions thrown from the supplier because the check is skipped
        options.enable(check_filesystem_ownership);
        CassandraRelevantProperties.FILE_SYSTEM_CHECK_ENABLE.setBoolean(false);
        AbstractFilesystemOwnershipCheckTest.checker(() -> { throw new RuntimeException("FAIL"); }).execute(options);
    }

    @Test
    public void checkEnabledButClusterPropertyIsEmpty()
    {
        CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.setString("");
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(tempDir), options, MISSING_PROPERTY, CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.getKey());
    }

    @Test
    public void checkEnabledButClusterPropertyIsUnset()
    {
        Assume.assumeFalse(options.getConfig(check_filesystem_ownership).containsKey("ownership_token"));
        CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.clearValue(); // checkstyle: suppress nearby 'clearValueSystemPropertyUsage'
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(tempDir), options, MISSING_PROPERTY, CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_TOKEN.getKey());
    }

    // tests for presence/absence of files in dirs
    @Test
    public void noRootDirectoryPresent() throws Exception
    {
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker("/no/such/location"), options, NO_OWNERSHIP_FILE, "'/no/such/location'");
    }

    @Test
    public void noDirectoryStructureOrTokenFilePresent() throws Exception
    {
        // The root directory exists, but is completely empty
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(tempDir), options, NO_OWNERSHIP_FILE, quote(tempDir.absolutePath()));
    }

    @Test
    public void directoryStructureButNoTokenFiles() throws Exception
    {
        File childDir = new File(tempDir, "cassandra/data"); //checkstyle: permit this instantiation
        assertTrue(childDir.tryCreateDirectories());
        assertTrue(childDir.exists());
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(childDir), options, NO_OWNERSHIP_FILE, quote(childDir.absolutePath()));
    }

    @Test
    public void multipleFilesFoundInSameTree() throws Exception
    {
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir, 1, token);
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir.parent(), 1, token);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir), options, MULTIPLE_OWNERSHIP_FILES, leafDir);
    }

    @Test
    public void singleValidFileInEachTree() throws Exception
    {
        // Happy path. Each target directory has exactly 1 token file in the
        // dir above it, they all contain the supplied token and the correct
        // count.
        File[] leafDirs = new File[] { AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d1/data"),
                                       AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d2/commitlogs"),
                                       AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d3/hints") };
        for (File dir : leafDirs)
            AbstractFilesystemOwnershipCheckTest.writeFile(dir.parent(), 3, token);
        AbstractFilesystemOwnershipCheckTest.checker(leafDirs).execute(options);
    }

    @Test
    public void multipleDirsSingleTree() throws Exception
    {
        // Happy path. Each target directory has exactly 1 token file in the
        // dir above it (as they all share a single parent). Each contains
        // the supplied token and the correct count (1 in this case).
        File[] leafDirs = new File[] { AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d1/data"),
                                       AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d2/commitlogs"),
                                       AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d3/hints") };
        AbstractFilesystemOwnershipCheckTest.writeFile(tempDir, 1, token);
        AbstractFilesystemOwnershipCheckTest.checker(leafDirs).execute(options);
    }

    @Test
    public void someDirsContainNoFile() throws Exception
    {
        File leafDir1 = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir1, 3, token);
        File leafDir2 = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/commitlogs");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir2, 3, token);
        File leafDir3 = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/hints");

        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir1, leafDir2, leafDir3),
                                                            options,
                                                            NO_OWNERSHIP_FILE,
                                                            quote(leafDir3.absolutePath()));
    }

    @Test
    public void propsFileUnreadable() throws Exception
    {
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        File tokenFile = AbstractFilesystemOwnershipCheckTest.writeFile(leafDir.parent(), 1, token);
        assertTrue(tokenFile.trySetReadable(false));
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            READ_EXCEPTION,
                                                            leafDir.absolutePath());
    }

    @Test
    public void propsFileIllegalContent() throws Exception
    {
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        File propsFile = new File(leafDir, DEFAULT_FS_OWNERSHIP_FILENAME); //checkstyle: permit this instantiation
        assertTrue(propsFile.createFileIfNotExists());
        try (OutputStream os = Files.newOutputStream(propsFile.toPath()))
        {
            os.write(AbstractFilesystemOwnershipCheckTest.makeRandomString(40).getBytes());
        }
        assertTrue(propsFile.isReadable());
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, VERSION),
                                                            leafDir.absolutePath());
    }

    @Test
    public void propsParentDirUnreadable() throws Exception
    {
        // The props file itself is readable, but its dir is not
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir, 1, token);
        assertTrue(leafDir.trySetReadable(false));
        AbstractFilesystemOwnershipCheckTest.checker(leafDir).execute(options);
    }

    @Test
    public void propsParentDirUntraversable() throws Exception
    {
        // top level dir can't be listed, so no files are found
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir.parent(), 1, token);
        assertTrue(tempDir.trySetExecutable(false));
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            NO_OWNERSHIP_FILE,
                                                            quote(leafDir.absolutePath()));
    }

    @Test
    public void overrideFilename() throws Exception
    {
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), "other_file", AbstractFilesystemOwnershipCheckTest.makeProperties(1, 1, token));
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir), options, NO_OWNERSHIP_FILE, quote(leafDir.absolutePath()));
        CassandraRelevantProperties.FILE_SYSTEM_CHECK_OWNERSHIP_FILENAME.setString("other_file");
        AbstractFilesystemOwnershipCheckTest.checker(leafDir).execute(options);
    }

    // check consistency between discovered files
    @Test
    public void differentTokensFoundInTrees() throws Exception
    {
        File file1 = AbstractFilesystemOwnershipCheckTest.writeFile(AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d1/data"), 3, token);
        File file2 = AbstractFilesystemOwnershipCheckTest.writeFile(AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d2/commitlogs"), 3, token);
        File file3 = AbstractFilesystemOwnershipCheckTest.writeFile(AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d3/hints"), 3, "mismatchingtoken");
        String errorSuffix = String.format("['%s', '%s'], ['%s']",
                                           file1.absolutePath(),
                                           file2.absolutePath(),
                                           file3.absolutePath());

        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(file1.parent(), file2.parent(), file3.parent()),
                                                            options,
                                                            INCONSISTENT_FILES_FOUND,
                                                            errorSuffix);
    }

    @Test
    public void differentExpectedCountsFoundInTrees() throws Exception
    {
        File file1 = AbstractFilesystemOwnershipCheckTest.writeFile(AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d1/data"), 1, token);
        File file2 = AbstractFilesystemOwnershipCheckTest.writeFile(AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d2/commitlogs"), 2, token);
        File file3 = AbstractFilesystemOwnershipCheckTest.writeFile(AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d3/hints"), 3, "mismatchingtoken");
        String errorSuffix = String.format("['%s'], ['%s'], ['%s']",
                                           file1.absolutePath(),
                                           file2.absolutePath(),
                                           file3.absolutePath());
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(file1.parent(), file2.parent(), file3.parent()),
                                                            options,
                                                            INCONSISTENT_FILES_FOUND,
                                                            errorSuffix);
    }

    // tests on property values in discovered files
    @Test
    public void emptyPropertiesFile() throws Exception
    {
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), DEFAULT_FS_OWNERSHIP_FILENAME, new Properties());
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, VERSION),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void missingVersionProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VOLUME_COUNT, "1");
        p.setProperty(TOKEN, "foo");
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, VERSION),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void nonNumericVersionProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "abc");
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, VERSION),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void unsupportedVersionProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "99");
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(UNSUPPORTED_VERSION, "99"),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void missingVolumeCountProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "1");
        p.setProperty(TOKEN, token);
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, VOLUME_COUNT),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void nonNumericVolumeCountProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "1");
        p.setProperty(VOLUME_COUNT, "bar");
        p.setProperty(TOKEN, token);
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, VOLUME_COUNT),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void missingTokenProp() throws Exception
    {
        Properties p = new Properties();
        p.setProperty(VERSION, "1");
        p.setProperty(VOLUME_COUNT, "1");
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        writeFile(leafDir.parent(), DEFAULT_FS_OWNERSHIP_FILENAME, p);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, TOKEN),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void emptyTokenProp() throws Exception
    {
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir.parent(), 1, "");
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            String.format(INVALID_PROPERTY_VALUE, TOKEN),
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    @Test
    public void mismatchingTokenProp() throws Exception
    {
        // Ownership token file exists in parent, but content doesn't match property
        File leafDir = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "cassandra/data");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir.parent(), 1, AbstractFilesystemOwnershipCheckTest.makeRandomString(15));
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir),
                                                            options,
                                                            MISMATCHING_TOKEN,
                                                            leafDir.parent().toPath().resolve(DEFAULT_FS_OWNERSHIP_FILENAME));
    }

    // Validate volume_count prop values match number of files found
    @Test
    public void expectedVolumeCountMoreThanActual() throws Exception
    {
        // The files on disk indicate that we should expect 2 ownership files,
        // but we only read 1, implying a disk mount is missing
        File[] leafDirs = new File[] { AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d1/data"),
                                       AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d2/commitlogs"),
                                       AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d3/hints") };
        AbstractFilesystemOwnershipCheckTest.writeFile(tempDir, 2, token);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDirs), options, INVALID_FILE_COUNT);
    }

    @Test
    public void expectedVolumeCountLessThanActual() throws Exception
    {
        // The files on disk indicate that we should expect 1 ownership file,
        // but we read 2, implying a extra unexpected disk mount is mounted
        File leafDir1 = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d1/data");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir1, 1, token);
        File leafDir2 = AbstractFilesystemOwnershipCheckTest.mkdirs(tempDir, "d2/commitlogs");
        AbstractFilesystemOwnershipCheckTest.writeFile(leafDir2, 1, token);
        AbstractFilesystemOwnershipCheckTest.executeAndFail(AbstractFilesystemOwnershipCheckTest.checker(leafDir1, leafDir2), options, INVALID_FILE_COUNT);
    }

    private String quote(String toQuote)
    {
        return String.format("'%s'", toQuote);
    }
}
