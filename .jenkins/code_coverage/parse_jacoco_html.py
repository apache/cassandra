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

parser = argparse.ArgumentParser(description='Parse Jacoco HTML report.')
parser.add_argument("-p", "--path", required=True, help='path of the Jacoco HTML report')

class Counter():
    def __init__(self, name: str):
        self.name = name

    def set_miss(self, miss: int):
        self.miss = miss

    def set_count(self, count: int):
        self.count = count

    def set_cov(self, cov: float):
        assert type(cov) in [float, str]
        # percentage string, e.g. 20%
        if type(cov) is float:
            self.coverage = f"{100 * cov:.2f}%"
        else:
            self.coverage = cov

    def cal_cov(self):
        if self.count == 0:
            self.coverage = "0.0%"
        elif hasattr(self, "coverage"):
            return
        else:
            self.coverage = f"{100.0 * (1 - float(self.miss) / float(self.count)):.2f}%"

    def get_cov(self):
        if not hasattr(self, "coverage"):
            self.cal_cov()
        return "**" + self.coverage + "**"

class JacocoReportRow():
    def __init__(self, package: str):
        self.package = package
        self.instructions = Counter("instructions")
        self.branches = Counter("branches")
        self.cxty = Counter("cxty")
        self.lines = Counter("lines")
        self.methods = Counter("methods")
        self.classes = Counter("classes")
        self.types = ["instructions", "branches", "cxty", "lines", "methods", "classes"]

    def get_attr_by_name(self, name):
        return getattr(self, name)

    def set_counter(self, name: str, **kwargs):
        assert name in self.types
        if name == "instructions":
            self.instructions.set_cov(kwargs["coverage"])
        elif name == "branches":
            self.branches.set_cov(kwargs["coverage"])
        elif name == "cxty":
            self.cxty.set_miss(kwargs["missed"])
            self.cxty.set_count(kwargs["count"])
        elif name == "lines":
            self.lines.set_miss(kwargs["missed"])
            self.lines.set_count(kwargs["count"])
        elif name == "methods":
            self.methods.set_miss(kwargs["missed"])
            self.methods.set_count(kwargs["count"])
        elif name == "classes":
            self.classes.set_miss(kwargs["missed"])
            self.classes.set_count(kwargs["count"])

    def get_coverage_row(self, cols):
        assert set(cols).issubset(["package"] + self.types)
        cov_row = []
        for col in cols:
            if col == "package":
                cov_row.append("##" + self.package + "##")
            else:
                cov_row.append(getattr(self, col).get_cov())
        return cov_row



class JacocoReport():
    def __init__(self):
        self.rows = []
        self.cols = ["package", "lines", "instructions", "branches", "classes"]

    def __str__(self):
        # phab comment style
        report_str = ""
        report_str += self.gen_header()
        self.rows = sorted(self.rows, key=lambda x: x.package)
        for row in self.rows:
            row_content = row.get_coverage_row(self.cols)
            report_str += '|' + '|'.join(row_content) + '|' + '\n'
        return report_str

    def gen_header(self):
        header = '|' + '|'.join(self.cols) + '|' + '\n'
        header += '|' + '|'.join(["----"] * len(self.cols)) + '|' + '\n'
        return header

    def add_row(self, row: JacocoReportRow):
        self.rows.append(row)

def parse_jacoco_html(df):
    report = JacocoReport()
    for _, v in df.iterrows():
        row = JacocoReportRow(package=v["element"])
        row.set_counter("instructions", coverage=v["instructions-Coverage"])
        row.set_counter("branches", coverage=v["branches-Coverage"])
        row.set_counter("cxty", missed=v["cxty-Missed"], count=v["cxty-Count"])
        row.set_counter("lines", missed=v["lines-Missed"], count=v["lines-Count"])
        row.set_counter("methods", missed=v["methods-Missed"], count=v["methods-Count"])
        row.set_counter("classes", missed=v["classes-Missed"], count=v["classes-Count"])
        report.add_row(row)
    return report

def main(args):
    html_df = pd.read_html(args.path)[0]
    html_df.columns = [
        "element",
        "instructions",
        "instructions-Coverage",
        "branches",
        "branches-Coverage",
        "cxty-Missed",
        "cxty-Count",
        "lines-Missed",
        "lines-Count",
        "methods-Missed",
        "methods-Count",
        "classes-Missed",
        "classes-Count"
    ]
    html_df = html_df.sort_values(by=["element"]).reset_index(drop=True)
    html_df = html_df.drop(["instructions", "branches"], axis=1)
    report = parse_jacoco_html(html_df)
    print(report)

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)