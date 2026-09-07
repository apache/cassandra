#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Test nodetool shell completion generation without starting Cassandra."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from xml.sax.saxutils import escape


CASSANDRA_DIR = Path(__file__).resolve().parents[3]
BUILD_DIR = CASSANDRA_DIR / os.environ.get("BUILD_DIR", "build")


class NodetoolCompletionTest(unittest.TestCase):
    def setUp(self):
        jars = list((BUILD_DIR / "lib" / "jars").glob("picocli-*.jar"))
        self.assertEqual(1, len(jars), "Build Cassandra first to resolve Picocli")
        self.tmp = tempfile.TemporaryDirectory(prefix="nodetool completion ")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.helpers = self.root / "helpers"
        self.helpers.mkdir()
        for name in ("build-autocomplete.xml", "header.txt"):
            shutil.copy(CASSANDRA_DIR / ".build" / name, self.helpers / name)
        self.lib = self.root / "lib"
        self.lib.mkdir()
        self.picocli = self.lib / jars[0].name
        shutil.copy(jars[0], self.picocli)
        self.source = self.root / "src/org/apache/cassandra/tools/nodetool/NodetoolCommand.java"
        self.source.parent.mkdir(parents=True)
        self.write_command("--ignore")
        self.classes = self.root / "classes"
        self.output = self.root / "custom-build/autocomplete/nodetool"
        (self.root / "build.xml").write_text(f'''<project>
    <property name="build.dir" location="custom-build"/>
    <property name="tmp.dir" location="${{build.dir}}/tmp"/>
    <property name="build.classes.main" location="classes"/>
    <property name="build.dir.lib" location="lib"/>
    <property name="build.helpers.dir" location="helpers"/>
    <path id="cassandra.classpath">
        <pathelement location="classes"/>
        <pathelement location="{escape(str(self.picocli))}"/>
    </path>
    <target name="build">
        <mkdir dir="classes"/>
        <javac srcdir="src" destdir="classes" includeantruntime="false"
               classpathref="cassandra.classpath"/>
    </target>
    <import file="helpers/build-autocomplete.xml"/>
</project>
''')

    def write_command(self, option):
        self.source.write_text('''package org.apache.cassandra.tools.nodetool;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
@Command(name = "nodetool", subcommands = NodetoolCommand.Cms.class)
public class NodetoolCommand {
    @Command(name = "cms", subcommands = Reconfigure.class)
    public static class Cms {}
    @Command(name = "reconfigure")
    public static class Reconfigure {
        @Option(names = "OPTION") String ignore;
    }
}
'''.replace("OPTION", option))

    def generate(self, success=True):
        result = subprocess.run(["ant", "-noinput", "-f", str(self.root / "build.xml"),
                                 "-Dbuild.dir=" + str(self.root / "custom-build"),
                                 "gen-autocomplete"], capture_output=True, text=True)
        self.assertEqual(success, result.returncode == 0, result.stdout + result.stderr)
        return result.stdout + result.stderr

    def complete(self, words):
        result = subprocess.run(["bash", "-c", '''
source "$1"
shift
COMP_WORDS=("$@")
COMP_CWORD=$((${#COMP_WORDS[@]} - 1))
COMP_LINE="${COMP_WORDS[*]}"
_complete_nodetool
printf '%s\n' "${COMPREPLY[@]}"
''', "bash", str(self.output), *words], capture_output=True, text=True, check=True)
        return result.stdout.splitlines()

    def test_generation_and_shell_completion(self):
        self.generate()
        text = self.output.read_text()
        self.assertTrue(text.startswith("#!/usr/bin/env bash\n#\n# Licensed"))
        self.assertEqual(1, text.count("#!/usr/bin/env bash"))
        self.assertEqual(["cms"], self.complete(["nodetool", "c"]))
        self.assertEqual(["reconfigure"], self.complete(["nodetool", "cms", "r"]))
        self.assertEqual(["--ignore"], self.complete(["nodetool", "cms", "reconfigure", "--i"]))
        for shell in ("bash", "zsh"):
            if shutil.which(shell):
                subprocess.run([shell, "-n", str(self.output)], check=True)
        if shutil.which("zsh"):
            result = subprocess.run(["zsh", "-fc", '''
autoload -Uz compinit
compinit -D
source "$1"
words=(nodetool cms reconfigure --i)
CURRENT=4
COMP_WORDS=("${words[@]}")
COMP_CWORD=3
COMP_LINE="${words[*]}"
compgen -F _complete_nodetool -- --i
''', "zsh", str(self.output)], capture_output=True, text=True, check=True)
            self.assertIn("--ignore", result.stdout.splitlines())

    def test_unchanged_and_unrelated_classes_skip_generation(self):
        self.generate()
        before = self.output.stat().st_mtime_ns
        unrelated = self.classes / "other/Unrelated.class"
        unrelated.parent.mkdir()
        unrelated.touch()
        self.generate()
        self.assertEqual(before, self.output.stat().st_mtime_ns)

    def test_changed_command_updates_completion(self):
        self.generate()
        os.utime(self.output, (1, 1))
        self.write_command("--exclude")
        command = self.classes / "org/apache/cassandra/tools/nodetool/NodetoolCommand.class"
        os.utime(command, (1, 1))
        self.generate()
        self.assertEqual(["--exclude"], self.complete(["nodetool", "cms", "reconfigure", "--"]))

    def test_generation_inputs_invalidate_output(self):
        self.generate()
        for changed in (self.helpers / "header.txt", self.helpers / "build-autocomplete.xml",
                        self.picocli, self.classes / "org/apache/cassandra/tools/nodetool/NodetoolCommand$Reconfigure.class"):
            with self.subTest(changed=changed.name):
                for path in self.root.rglob("*"):
                    if path.is_file():
                        os.utime(path, (100, 100))
                os.utime(self.output, (200, 200))
                changed.touch()
                self.generate()
                self.assertGreater(self.output.stat().st_mtime, 200)

    def test_missing_output_is_regenerated(self):
        self.generate()
        expected = self.output.read_bytes()
        self.output.unlink()
        self.generate()
        self.assertEqual(expected, self.output.read_bytes())

    def test_failed_generation_preserves_previous_output(self):
        self.generate()
        before = self.output.read_bytes()
        os.utime(self.output, (1, 1))
        self.source.write_text("package org.apache.cassandra.tools.nodetool; public class NodetoolCommand {}")
        command = self.classes / "org/apache/cassandra/tools/nodetool/NodetoolCommand.class"
        os.utime(command, (1, 1))
        output = self.generate(success=False)
        self.assertIn("Java returned: 4", output)
        self.assertEqual(before, self.output.read_bytes())
        self.assertEqual(1, self.output.stat().st_mtime)