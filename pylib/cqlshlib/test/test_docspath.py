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

import os
import tempfile
from unittest.mock import patch

from .basecase import BaseTestCase
from cqlshlib.cqlshmain import get_docspath, _get_docs_from_package_resource, Shell


class TestGetDocspath(BaseTestCase):
    """
    Tests for the get_docspath() function.

    Verifies that CQL documentation paths are resolved according to the
    function's priority logic.
    """

    def test_local_dev_path(self):
        """Local doc/cql3/CQL.html takes precedence over all other paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            docs_dir = os.path.join(tmpdir, 'doc', 'cql3')
            os.makedirs(docs_dir)
            docs_file = os.path.join(docs_dir, 'CQL.html')
            with open(docs_file, 'w') as f:
                f.write('<html></html>')

            result = get_docspath(tmpdir)

            self.assertTrue(result.startswith('file://'))
            self.assertIn('doc/cql3/CQL.html', result)
            self.assertEqual(result, 'file://' + os.path.abspath(docs_file))

    def test_linux_package_path(self):
        """Linux package path when local path doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('os.path.exists') as mock_exists:
                def exists_side_effect(path):
                    if path == os.path.join(tmpdir, 'doc', 'cql3', 'CQL.html'):
                        return False
                    if path == '/usr/share/doc/cassandra/CQL.html':
                        return True
                    return False

                mock_exists.side_effect = exists_side_effect

                result = get_docspath(tmpdir)

                self.assertEqual(result, 'file:///usr/share/doc/cassandra/CQL.html')

    def test_macos_path(self):
        """macOS path when local and Linux paths don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('os.path.exists') as mock_exists:
                def exists_side_effect(path):
                    if path == os.path.join(tmpdir, 'doc', 'cql3', 'CQL.html'):
                        return False
                    if path == '/usr/share/doc/cassandra/CQL.html':
                        return False
                    if path == '/usr/local/share/doc/cassandra/CQL.html':
                        return True
                    return False

                mock_exists.side_effect = exists_side_effect

                result = get_docspath(tmpdir)

                self.assertEqual(result, 'file:///usr/local/share/doc/cassandra/CQL.html')

    def test_package_resource(self):
        """Package resource when filesystem paths don't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('os.path.exists', return_value=False):
                with patch('cqlshlib.cqlshmain._get_docs_from_package_resource') as mock_resource:
                    mock_resource.return_value = 'file:///some/resource/path/CQL.html'

                    result = get_docspath(tmpdir)

                    self.assertEqual(result, 'file:///some/resource/path/CQL.html')
                    mock_resource.assert_called_once()

    def test_online_url_fallback(self):
        """Online documentation URL when all local paths fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('os.path.exists', return_value=False):
                with patch('cqlshlib.cqlshmain._get_docs_from_package_resource', return_value=None):
                    result = get_docspath(tmpdir)

                    self.assertEqual(result, Shell.DEFAULT_CQLDOCS_URL)


class TestGetDocsFromPackageResource(BaseTestCase):
    """Tests for the _get_docs_from_package_resource() function."""

    def test_returns_none_on_import_error(self):
        """Should return None if importlib.resources is not available."""
        with patch.dict('sys.modules', {'importlib.resources': None}):
            with patch('cqlshlib.cqlshmain.sys.version_info', (3, 9)):
                with patch('builtins.__import__', side_effect=ImportError):
                    result = _get_docs_from_package_resource()
                    self.assertIsNone(result)

    def test_returns_none_when_resource_not_found(self):
        """Should return None if the resource file doesn't exist on filesystem."""
        from unittest.mock import MagicMock

        with patch('cqlshlib.cqlshmain.sys.version_info', (3, 9)):
            with patch('importlib.resources.files') as mock_files:
                mock_files.return_value.joinpath.return_value = '/wrong/path/CQL.html'
                result = _get_docs_from_package_resource()
                self.assertIsNone(result)

    def test_returns_file_url_when_resource_exists(self):
        """Should return file:// URL when resource exists on filesystem."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resource_file = os.path.join(tmpdir, 'CQL.html')
            with open(resource_file, 'w') as f:
                f.write('<html></html>')

            with patch('cqlshlib.cqlshmain.sys.version_info', (3, 9)):
                with patch('importlib.resources.files') as mock_files:
                    mock_files.return_value.joinpath.return_value = resource_file
                    result = _get_docs_from_package_resource()
                    self.assertEqual(result, 'file://' + os.path.realpath(resource_file))

    def test_exception_handling(self):
        """Should handle exceptions gracefully and return None."""
        with patch('cqlshlib.cqlshmain.sys.version_info', (3, 9)):
            with patch('importlib.resources.files', side_effect=Exception("Test error")):
                result = _get_docs_from_package_resource()
                self.assertIsNone(result)

    def test_python38_returns_none_on_import_error(self):
        """Should return None if importlib.util is not available on Python 3.8."""
        with patch.dict('sys.modules', {'importlib.util': None}):
            with patch('cqlshlib.cqlshmain.sys.version_info', (3, 8)):
                with patch('builtins.__import__', side_effect=ImportError):
                    result = _get_docs_from_package_resource()
                    self.assertIsNone(result)

    def test_python38_returns_none_when_spec_not_found(self):
        """Should return None if package spec is not found on Python 3.8."""
        with patch('cqlshlib.cqlshmain.sys.version_info', (3, 8)):
            with patch('importlib.util.find_spec', return_value=None):
                result = _get_docs_from_package_resource()
                self.assertIsNone(result)

    def test_python38_returns_none_when_resource_not_found(self):
        """Should return None if the resource file doesn't exist on Python 3.8."""
        from unittest.mock import MagicMock

        mock_spec = MagicMock()
        mock_spec.origin = '/wrong/package/__init__.py'

        with patch('cqlshlib.cqlshmain.sys.version_info', (3, 8)):
            with patch('importlib.util.find_spec', return_value=mock_spec):
                result = _get_docs_from_package_resource()
                self.assertIsNone(result)

    def test_python38_returns_file_url_when_resource_exists(self):
        """Should return file:// URL when resource exists on Python 3.8."""
        from unittest.mock import MagicMock

        with tempfile.TemporaryDirectory() as tmpdir:
            resource_file = os.path.join(tmpdir, 'CQL.html')
            with open(resource_file, 'w') as f:
                f.write('<html></html>')

            mock_spec = MagicMock()
            mock_spec.origin = os.path.join(tmpdir, '__init__.py')

            with patch('cqlshlib.cqlshmain.sys.version_info', (3, 8)):
                with patch('importlib.util.find_spec', return_value=mock_spec):
                    result = _get_docs_from_package_resource()
                    self.assertEqual(result, 'file://' + os.path.realpath(resource_file))

    def test_python38_exception_handling(self):
        """Should handle exceptions gracefully and return None on Python 3.8."""
        with patch('cqlshlib.cqlshmain.sys.version_info', (3, 8)):
            with patch('importlib.util.find_spec', side_effect=Exception("Test error")):
                result = _get_docs_from_package_resource()
                self.assertIsNone(result)
