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
package org.apache.cassandra.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.ArrayUtils;

import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.VersionNumber;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Generate;
import org.quicktheories.generators.SourceDSL;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.matchers.JUnitMatchers.containsString;
import static org.quicktheories.QuickTheory.qt;

public class CassandraVersionTest
{
    @Test
    public void toStringParses()
    {
        qt().forAll(versionGen()).checkAssert(version -> {
            Assertions.assertThat(new CassandraVersion(version.toString()))
                      .isEqualTo(version)
                      .hasSameHashCodeAs(version)
                      .isEqualByComparingTo(version);

        });
    }

    @Test
    public void clientCanParse()
    {
        qt().forAll(versionGen()).checkAssert(version -> {
            Assertions.assertThat(VersionNumber.parse(version.toString())).isNotNull();
        });
    }

    private static Gen<CassandraVersion> versionGen()
    {
        Gen<Integer> positive = SourceDSL.integers().allPositive();
        Gen<Integer> hotfixGen = positive.mix(Generate.constant(CassandraVersion.NO_HOTFIX));
        Gen<Integer> smallSizes = SourceDSL.integers().between(0, 5);
        Gen<String> word = Generators.regexWord(SourceDSL.integers().between(1, 100)); // empty isn't allowed while parsing since \w+ is used, so must be at least 1
        return td -> {
            int major = positive.generate(td);
            int minor = positive.generate(td);
            int patch = positive.generate(td);

            int hotfix = hotfixGen.generate(td);

            int numPreRelease = smallSizes.generate(td);
            String[] preRelease = numPreRelease == 0 ? null : new String[numPreRelease];
            for (int i = 0; i < numPreRelease; i++)
                preRelease[i] = word.generate(td);

            int numBuild = smallSizes.generate(td);
            String[] build = numBuild == 0 ? null : new String[numBuild];
            for (int i = 0; i < numBuild; i++)
                build[i] = word.generate(td);
            return new CassandraVersion(major, minor, patch, hotfix, preRelease, build);
        };
    }

    @Test
    public void multiplePreRelease()
    {
        for (String version : Arrays.asList("4.0-alpha1-SNAPSHOT",
                                            "4.0.1-alpha1-SNAPSHOT",
                                            "4.0.1.1-alpha1-SNAPSHOT",
                                            "4.0.0.0-a-b-c-d-e-f-g"))
        {
            CassandraVersion cassandra = new CassandraVersion(version);
            VersionNumber client = VersionNumber.parse(version);
            Assert.assertEquals(cassandra.major, client.getMajor());
            Assert.assertEquals(cassandra.minor, client.getMinor());
            Assert.assertEquals(cassandra.patch, client.getPatch());
            Assert.assertEquals(cassandra.hotfix, client.getDSEPatch());
            Assert.assertEquals(cassandra.getPreRelease(), client.getPreReleaseLabels());
        }
    }

    @Test
    public void multipleBuild()
    {
        for (String version : Arrays.asList("4.0+alpha1.SNAPSHOT",
                                            "4.0.1+alpha1.SNAPSHOT",
                                            "4.0.1.1+alpha1.SNAPSHOT",
                                            "4.0.0.0+a.b.c.d.e.f.g"))
        {
            CassandraVersion cassandra = new CassandraVersion(version);
            VersionNumber client = VersionNumber.parse(version);
            Assert.assertEquals(cassandra.major, client.getMajor());
            Assert.assertEquals(cassandra.minor, client.getMinor());
            Assert.assertEquals(cassandra.patch, client.getPatch());
            Assert.assertEquals(cassandra.hotfix, client.getDSEPatch());
            Assert.assertEquals(cassandra.getBuild(), Splitter.on(".").splitToList(client.getBuildLabel()));
        }
    }

    @Test
    public void testParsing()
    {
        CassandraVersion version;

        version = new CassandraVersion("1.2.3");
        assertTrue(version.major == 1 && version.minor == 2 && version.patch == 3);

        version = new CassandraVersion("1.2.3-foo.2+Bar");
        assertTrue(version.major == 1 && version.minor == 2 && version.patch == 3);

        // CassandraVersion can parse 4th '.' as build number
        version = new CassandraVersion("1.2.3.456");
        assertTrue(version.major == 1 && version.minor == 2 && version.patch == 3);

        // support for tick-tock release
        version = new CassandraVersion("3.2");
        assertTrue(version.major == 3 && version.minor == 2 && version.patch == 0);
    }

    @Test
    public void testCompareTo()
    {
        CassandraVersion v1, v2;

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.4");
        assertTrue(v1.compareTo(v2) < 0);

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3");
        assertTrue(v1.compareTo(v2) == 0);

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("2.0.0");
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3-alpha");
        assertTrue(v1.compareTo(v2) > 0);

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3+foo");
        assertTrue(v1.compareTo(v2) < 0);

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3-alpha+foo");
        assertTrue(v1.compareTo(v2) > 0);

        v1 = new CassandraVersion("1.2.3-alpha+1");
        v2 = new CassandraVersion("1.2.3-alpha+2");
        assertTrue(v1.compareTo(v2) < 0);

        v1 = new CassandraVersion("4.0-rc2");
        v2 = new CassandraVersion("4.0-rc1");
        assertTrue(v1.compareTo(v2) > 0);
        assertTrue(v2.compareTo(v1) < 0);

        v1 = new CassandraVersion("4.0-rc2");
        v2 = new CassandraVersion("4.0-rc1");
        assertTrue(v1.compareTo(v2) > 0);
        assertTrue(v2.compareTo(v1) < 0);

        v1 = new CassandraVersion("4.0.0");
        v2 = new CassandraVersion("4.0-rc2");
        assertTrue(v1.compareTo(v2) > 0);
        assertTrue(v2.compareTo(v1) < 0);

        v1 = new CassandraVersion("1.2.3-SNAPSHOT");
        v2 = new CassandraVersion("1.2.3-alpha");
        assertTrue(v1.compareTo(v2) > 0);
        assertTrue(v2.compareTo(v1) < 0);

        v1 = new CassandraVersion("1.2.3-SNAPSHOT");
        v2 = new CassandraVersion("1.2.3-alpha-SNAPSHOT");
        assertTrue(v1.compareTo(v2) > 0);
        assertTrue(v2.compareTo(v1) < 0);

        v1 = new CassandraVersion("1.2.3-SNAPSHOT");
        v2 = new CassandraVersion("1.2.3");
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);

        v1 = new CassandraVersion("1.2-SNAPSHOT");
        v2 = new CassandraVersion("1.2.3");
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);

        v1 = new CassandraVersion("1.2.3-SNAPSHOT");
        v2 = new CassandraVersion("1.2");
        assertTrue(v1.compareTo(v2) > 0);
        assertTrue(v2.compareTo(v1) < 0);

        v1 = new CassandraVersion("1.2-rc2");
        v2 = new CassandraVersion("1.2.3-SNAPSHOT");
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);

        v1 = new CassandraVersion("1.2.3-rc2");
        v2 = new CassandraVersion("1.2-SNAPSHOT");
        assertTrue(v1.compareTo(v2) > 0);
        assertTrue(v2.compareTo(v1) < 0);

        v1 = CassandraVersion.CASSANDRA_4_0;
        v2 = new CassandraVersion("4.0.0-SNAPSHOT");
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);

        v1 = new CassandraVersion("4.0");
        v2 = new CassandraVersion("4.0.0");
        assertTrue(v1.compareTo(v2) == 0);
        assertTrue(v2.compareTo(v1) == 0);

        v1 = new CassandraVersion("4.0").familyLowerBound.get();
        v2 = new CassandraVersion("4.0.0");
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);

        v1 = new CassandraVersion("4.0").familyLowerBound.get();
        v2 = new CassandraVersion("4.0");
        assertTrue(v1.compareTo(v2) < 0);
        assertTrue(v2.compareTo(v1) > 0);

        assertEquals(-1, v1.compareTo(v2));

        v1 = new CassandraVersion("1.2.3");
        v2 = new CassandraVersion("1.2.3.1");
        assertEquals(-1, v1.compareTo(v2));

        v1 = new CassandraVersion("1.2.3.1");
        v2 = new CassandraVersion("1.2.3.2");
        assertEquals(-1, v1.compareTo(v2));
    }

    @Test
    public void testInvalid()
    {
        assertThrows("1.0.0a");
        assertThrows("1.a.4");
        assertThrows("1.0.0-foo&");
    }

    @Test
    public void testEquals()
    {
        assertEquals(new CassandraVersion("3.0"), new CassandraVersion("3.0.0"));
        assertNotEquals(new CassandraVersion("3.0"), new CassandraVersion("3.0").familyLowerBound.get());
        assertNotEquals(new CassandraVersion("3.0.0"), new CassandraVersion("3.0.0").familyLowerBound.get());
        assertNotEquals(new CassandraVersion("3.0.0"), new CassandraVersion("3.0").familyLowerBound.get());
    }

    @Test
    public void testFamilyLowerBound()
    {
        CassandraVersion expected = new CassandraVersion(3, 0, 0, CassandraVersion.NO_HOTFIX, ArrayUtils.EMPTY_STRING_ARRAY, null);
        assertEquals(expected, new CassandraVersion("3.0.0-alpha1-SNAPSHOT").familyLowerBound.get());
        assertEquals(expected, new CassandraVersion("3.0.0-alpha1").familyLowerBound.get());
        assertEquals(expected, new CassandraVersion("3.0.0-rc1-SNAPSHOT").familyLowerBound.get());
        assertEquals(expected, new CassandraVersion("3.0.0-rc1").familyLowerBound.get());
        assertEquals(expected, new CassandraVersion("3.0.0-SNAPSHOT").familyLowerBound.get());
        assertEquals(expected, new CassandraVersion("3.0.0").familyLowerBound.get());
        assertEquals(expected, new CassandraVersion("3.0.1-SNAPSHOT").familyLowerBound.get());
        assertEquals(expected, new CassandraVersion("3.0.1").familyLowerBound.get());
    }

    @Test
    public void testOrderWithSnapshotsAndFamilyLowerBound()
    {
        List<CassandraVersion> expected = Arrays.asList(new CassandraVersion("2.0").familyLowerBound.get(),
                                                        new CassandraVersion("2.1.5"),
                                                        new CassandraVersion("2.1.5.123"),
                                                        new CassandraVersion("2.2").familyLowerBound.get(),
                                                        new CassandraVersion("2.2.0-beta1-snapshot"),
                                                        new CassandraVersion("2.2.0-beta1"),
                                                        new CassandraVersion("2.2.0-beta2-SNAPSHOT"),
                                                        new CassandraVersion("2.2.0-beta2"),
                                                        new CassandraVersion("2.2.0-rc1-snapshot"),
                                                        new CassandraVersion("2.2.0-rc1"),
                                                        new CassandraVersion("2.2.0-SNAPSHOT"),
                                                        new CassandraVersion("2.2.0"),
                                                        new CassandraVersion("3.0").familyLowerBound.get(),
                                                        new CassandraVersion("3.0-alpha1"),
                                                        new CassandraVersion("3.0-alpha2-SNAPSHOT"),
                                                        new CassandraVersion("3.0-alpha2"),
                                                        new CassandraVersion("3.0-beta1"),
                                                        new CassandraVersion("3.0-beta2-SNAPSHOT"),
                                                        new CassandraVersion("3.0-beta2"),
                                                        new CassandraVersion("3.0-RC1-SNAPSHOT"),
                                                        new CassandraVersion("3.0-RC1"),
                                                        new CassandraVersion("3.0-RC2-SNAPSHOT"),
                                                        new CassandraVersion("3.0-RC2"),
                                                        new CassandraVersion("3.0-SNAPSHOT"),
                                                        new CassandraVersion("3.0.0"),
                                                        new CassandraVersion("3.0.1-SNAPSHOT"),
                                                        new CassandraVersion("3.0.1"),
                                                        new CassandraVersion("3.2").familyLowerBound.get(),
                                                        new CassandraVersion("3.2-SNAPSHOT"),
                                                        new CassandraVersion("3.2"));

        for (int i = 0; i < 100; i++)
        {
            List<CassandraVersion> shuffled = new ArrayList<>(expected);
            Collections.shuffle(shuffled);

            List<CassandraVersion> sorted = new ArrayList<>(shuffled);
            Collections.sort(sorted);
            if (!expected.equals(sorted))
            {
                fail("Expecting " + shuffled + " to be sorted into " + expected + " but was sorted into " + sorted);
            }
        }
    }

    private static void assertThrows(String str)
    {
        try
        {
            new CassandraVersion(str);
            fail();
        }
        catch (IllegalArgumentException e) {}
    }

    @Test
    public void testParseIdentifiersPositive() throws Throwable
    {
        String[] result = parseIdentifiers("DUMMY", "a.b.cde.f_g.");
        String[] expected = {"a", "b", "cde", "f_g"};
        assertArrayEquals(expected, result);
    }

    @Test
    public void testParseIdentifiersNegative() throws Throwable
    {
        String version = "DUMMY";
        try
        {
            parseIdentifiers(version, "+a. .b");

        }
        catch (IllegalArgumentException e)
        {
            assertThat(e.getMessage(), containsString(version));
        }
    }

    @Test
    public void testExtraOrdering()
    {
        List<CassandraVersion> versions = Arrays.asList(version("4.0.0"),
                                                        version("4.0.0-SNAPSHOT"),
                                                        version("4.0.0.0"),
                                                        version("4.0.0.0-SNAPSHOT"));
        List<CassandraVersion> expected = Arrays.asList(version("4.0.0-SNAPSHOT"),
                                                        version("4.0.0"),
                                                        version("4.0.0.0-SNAPSHOT"),
                                                        version("4.0.0.0"));
        Collections.sort(versions);
        Assertions.assertThat(versions).isEqualTo(expected);
    }

    private static CassandraVersion version(String str)
    {
        return new CassandraVersion(str);
    }

    private static String[] parseIdentifiers(String version, String str) throws Throwable
    {
        String name = "parseIdentifiers";
        Class[] args = {String.class, String.class};
        for (Method m: CassandraVersion.class.getDeclaredMethods())
        {
            if (name.equals(m.getName()) &&
                    Arrays.equals(args, m.getParameterTypes()))
            {
                m.setAccessible(true);
                try
                {
                return (String[]) m.invoke(null, version, str);
                } catch (InvocationTargetException e){
                    throw e.getTargetException();
                }
            }
        }
        throw new NoSuchMethodException(CassandraVersion.class + "." + name + Arrays.toString(args));
    }
}
