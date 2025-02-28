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

import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='Parse Jacoco report.')
parser.add_argument("--path", help='path of the Jacoco CSV report')
parser.add_argument("--diff_path", help='path of the diffCoverage CSV report')
parser.add_argument("--threshold", default=0.8, help='line coverage threashold')
parser.add_argument("--test", default="test", help='name of the test group in Cassandra (ANT_TARGET)')

class JacocoReport():
    def __init__(self,
                 test_group,
                 path,
                 cols=["package", "line", "line_count", "instruction", "branch", "complexity", "method"],
                 header="= Jacoco code coverage report for Cassandra (test_group={test_group}) = \n"):
        self.rows = []
        self.cols = cols
        self.test_group = test_group
        self.header = header.format(test_group=test_group)
        self.init_rows(path)

    def __str__(self):
        if len(self.rows) == 0:
            return self.header + "Report is empty \n"
        # phab comment style
        report_str = ""
        report_str += self.gen_header()
        for row in self.rows:
            report_str += '|' + '|'.join(self.get_row_content(row)) + '|' + '\n'
        return report_str

    def gen_header(self):
        header = self.header
        header += '|' + '|'.join(self.cols) + '|' + '\n'
        header += '|' + '|'.join(["----"] * len(self.cols)) + '|' + '\n'
        return header

    def get_row_content(self, row):
        str_list_row = [f"##{row[0]}##",            # package
                        f"**{row[1] * 100:.2f}%**", # line coverage
                        f"{row[2]}",                # line count
                        f"**{row[3] * 100:.2f}%**", # instruction coverage
                        f"**{row[4] * 100:.2f}%**", # branch coverage
                        f"**{row[5] * 100:.2f}%**", # complexity coverage
                        f"**{row[6] * 100:.2f}%**"  # method coverage
                        ]
        return str_list_row

    def init_rows(self, path):
        df = pd.read_csv(path)
        if df.empty:
            return
        df = df.groupby("PACKAGE", as_index=False).sum(["PACKAGE",
                                                        "INSTRUCTION_MISSED", "INSTRUCTION_COVERED",
                                                        "BRANCH_MISSED", "BRANCH_COVERED",
                                                        "LINE_MISSED", "LINE_COVERED",
                                                        "COMPLEXITY_MISSED", "COMPLEXITY_COVERED",
                                                        "METHOD_MISSED", "METHOD_COVERED"])

        def safe_divide(numerator, denominator):
            return numerator.divide(denominator).fillna(0)

        def compute_coverage(d):
            d["line_count"] = d["LINE_COVERED"] + d["LINE_MISSED"]
            d["line"] = safe_divide(d["LINE_COVERED"], d["line_count"])
            d["instruction"] = safe_divide(d["INSTRUCTION_COVERED"], d["INSTRUCTION_COVERED"] + d["INSTRUCTION_MISSED"])
            d["branch"] = safe_divide(d["BRANCH_COVERED"], d["BRANCH_COVERED"] + d["BRANCH_MISSED"])
            d["complexity"] = safe_divide(d["COMPLEXITY_COVERED"], d["COMPLEXITY_COVERED"] + d["COMPLEXITY_MISSED"])
            d["method"] = safe_divide(d["METHOD_COVERED"], d["METHOD_COVERED"] + d["METHOD_MISSED"])
            return d[["PACKAGE", "line", "line_count", "instruction", "branch", "complexity", "method"]]

        total = df[["INSTRUCTION_MISSED", "INSTRUCTION_COVERED",
                    "BRANCH_MISSED", "BRANCH_COVERED",
                    "LINE_MISSED", "LINE_COVERED",
                    "COMPLEXITY_MISSED", "COMPLEXITY_COVERED",
                    "METHOD_MISSED", "METHOD_COVERED"]].sum()
        total["PACKAGE"] = "Total"

        self.rows = compute_coverage(pd.DataFrame([total])).values.tolist()
        self.rows.extend(compute_coverage(df).values.tolist())

class JacocoDiffCoverageReport(JacocoReport):
    def __init__(self,
                 test_group,
                 path,
                 threshold,
                 cols=["package", "diff_new_line_coverage", "new_line_threshold", "new_line_check"],
                 header="= Diff coverage report to origin/master (test_group={test_group}) = \n"):
        super().__init__(test_group, path, cols, header)
        self.threshold = threshold

    def get_row_content(self, row):
        str_list_row = [f"##{row[0]}##",                                        # package
                        f"**{row[1]:.3f}**",                                    # diff new line coverage
                        f"{self.threshold:.3f}",                                # line coverage threshold
                        "**Pass**" if row[1] > self.threshold else "**Failed**" # pass check / fail check
                        ]
        return str_list_row


def main(args):
    if args.path is not None:
        report = JacocoReport(args.test, args.path)
        print(report)
    if args.diff_path is not None:
        diff_report = JacocoDiffCoverageReport(args.test, args.diff_path, float(args.threshold))
        print(diff_report)

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
