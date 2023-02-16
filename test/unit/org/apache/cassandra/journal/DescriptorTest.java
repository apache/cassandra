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
package org.apache.cassandra.journal;

import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.Files;
import org.apache.cassandra.io.util.PathUtils;
import org.apache.cassandra.utils.Pair;
import org.assertj.core.api.Condition;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

public class DescriptorTest
{
    private static final FileSystem FS = Files.newGlobalInMemoryFileSystem();

    static
    {
        PathUtils.setDeletionListener(ignore -> {});
    }

    @Test
    public void serde()
    {
        qt().forAll(descriptors())
            .check(desc ->
                   assertThat(Descriptor.fromFile(desc.fileFor(Component.DATA))).isEqualTo(desc));
    }

    @Test
    public void isTmp()
    {
        Condition<File> isTmp = new Condition<File>("isTmpFile")
        {
            @Override
            public boolean matches(File value)
            {
                return Descriptor.isTmpFile(value);
            }
        };
        qt().forAll(descriptors()).check(desc -> {
            for (Component comp : Component.values())
            {
                assertThat(desc.tmpFileFor(comp)).is(isTmp);
                assertThat(desc.fileFor(comp)).isNot(isTmp);
            }
        });
    }

    @Test
    public void list()
    {
        qt().withPure(false)
            .forAll(children())
            .check(pair ->
                   assertThat(Descriptor.list(pair.left)).containsExactlyInAnyOrderElementsOf(pair.right));
    }

    @Test
    public void order()
    {
        qt().withPure(false).forAll(children().filter(p -> p.right.size() >= 2)).check(pair ->
        {
            List<Descriptor> list = new ArrayList<>(pair.right);
            Collections.sort(list);

            Descriptor last = list.get(0);
            for (int i = 1; i < list.size(); i++)
            {
                Descriptor current = list.get(i);
                assertThat(current.directory).isEqualTo(last.directory);
                assertThat(current.timestamp).isGreaterThanOrEqualTo(last.timestamp);
                if (current.timestamp == last.timestamp)
                    assertThat(current.generation).isGreaterThanOrEqualTo(last.generation);
                if (current.timestamp == last.timestamp
                    && current.generation == last.generation)
                    assertThat(current.journalVersion).isGreaterThanOrEqualTo(last.journalVersion);
                if (current.timestamp == last.timestamp
                    && current.generation == last.generation
                    && current.journalVersion == last.journalVersion)
                    assertThat(current.userVersion).isGreaterThanOrEqualTo(last.userVersion);
                last = current;
            }
        });
    }

    private static Gen<Pair<File, Set<Descriptor>>> children()
    {
        Gen<File> dirs = dirs();
        return rs ->
        {
            File dir = dirs.next(rs);
            if (dir.exists())
                dir.deleteRecursive();
            if (!dir.createDirectoriesIfNotExists())
                throw new AssertionError("Directory " + dir + " exists");
            int size = rs.nextInt(0, 10);
            if (size == 0)
                return Pair.create(dir, Collections.emptySet());
            Set<Descriptor> uniq = Sets.newHashSetWithExpectedSize(size);
            Gen<Descriptor> descriptors = descriptors(Gens.constant(dir));
            for (int i = 0; i < size; i++)
            {
                Descriptor d = descriptors.next(rs);
                while (!uniq.add(d))
                    d = descriptors.next(rs);
            }
            for (Descriptor d : uniq)
                d.fileFor(Component.DATA).createFileIfNotExists();
            return Pair.create(dir, uniq);
        };
    }

    private static Gen<Descriptor> descriptors()
    {
        Gen<File> dir = dirs();
        return descriptors(dir);
    }

    private static Gen<Descriptor> descriptors(Gen<File> dir)
    {
        Gen.LongGen longs = Gens.longs().between(0, 10);
        Gen.IntGen ints = Gens.ints().between(0, 10);
        return rs -> new Descriptor(dir.next(rs), longs.nextLong(rs), ints.next(rs), ints.next(rs), ints.next(rs));
    }

    private static Gen<File> dirs()
    {
        Gen<String> names = asciiVisible().ofLengthBetween(1, 100);
        Gen<File> gen = rs -> new File(FS.getPath('/' + names.next(rs)));
        return gen.filter(f -> f.toCanonical().parent() != null);
    }

    // TODO: replace with Gens.strings().asciiVisible()
    public static Gens.SizeBuilder<String> asciiVisible()
    {
        return new Gens.SizeBuilder<>(sizes -> Gens.strings().betweenCodePoints(sizes, 33, 127));
    }
}
