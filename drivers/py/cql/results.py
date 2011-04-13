
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

class ResultSet(object):

    def __init__(self, rows, keyspace, column_family, decoder):
        self.rows = rows
        self.ks = keyspace
        self.cf = column_family
        self.decoder = decoder

        # We need to try to parse the first row to set the description
        if len(self.rows) > 0:
            self.description, self._first_vals = self.decoder.decode_row(self.ks, self.cf, self.rows[0])
        else:
            self.description, self._first_vals = (None, None)

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, idx):
        if isinstance(idx, int):
            if idx == 0 and self._first_vals:
                return self._first_vals
            self.description, vals = self.decoder.decode_row(self.ks, self.cf, self.rows[idx])
            return vals
        elif isinstance(idx, slice):
            num_rows = len(self.rows)
            results = []
            for i in xrange(idx.start, min(len(self.rows), idx.stop)):
                if i == 0 and self._first_vals:
                    vals = self._first_vals
                else:
                    self.description, vals = self.decoder.decode_row(self.ks, self.cf, self.rows[i])
                results.append(vals)
            return results
        else:
            raise TypeError

    def __iter__(self):
        idx = 0
        for r in self.rows:
            if idx == 0 and self._first_vals:
                yield self._first_vals
            else:
                self.description, vals = self.decoder.decode_row(self.ks, self.cf, r)
                yield vals
            idx += 1
