#!/bin/bash
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
# Validates the CI declarations under .jenkins/ without deploying anything:
#  - the Jenkinsfile parses as groovy
#  - jenkins-deployment.yaml renders through the jenkins helm chart
#  - the yaml embedded in it (agent pod templates, JCasC config scripts) parses
#
# Requires: helm, python3 with pyyaml, and either groovy or docker.
# Run from anywhere: .jenkins/k8s/jenkins-test.sh

set -e

CASSANDRA_DIR="$(cd "$(dirname "$0")/../.." > /dev/null && pwd)"
JENKINS_DIR="${CASSANDRA_DIR}/.jenkins"
status=0

command -v helm > /dev/null || { echo "helm must be installed and in the PATH"; exit 1; }
command -v python3 > /dev/null || { echo "python3 must be installed and in the PATH"; exit 1; }
python3 -c "import yaml" 2> /dev/null || { echo "python3 pyyaml must be installed: pip install pyyaml"; exit 1; }

echo "== Jenkinsfile groovy syntax"
# Phases.CONVERSION parses and builds the AST without resolving the pipeline DSL or @NonCPS,
# neither of which exist outside a jenkins controller
syntax_check_dir="$(mktemp -d)"
syntax_check="${syntax_check_dir}/syntax-check.groovy"
# mktemp gives 0700, which the unprivileged user inside the groovy image cannot traverse
chmod 755 "${syntax_check_dir}"
cat > "${syntax_check}" << 'EOF'
import org.codehaus.groovy.control.CompilationUnit
import org.codehaus.groovy.control.Phases

def cu = new CompilationUnit()
args.each { cu.addSource(new File(it)) }
cu.compile(Phases.CONVERSION)
println "  ${args.join(', ')} parses"
EOF
if command -v groovy > /dev/null ; then
  groovy "${syntax_check}" "${JENKINS_DIR}/Jenkinsfile" || status=1
elif command -v docker > /dev/null ; then
  # absolute paths, the image's working directory is not where the script was mounted
  docker run --rm -v "${syntax_check_dir}:/check:ro" -v "${JENKINS_DIR}:/jenkins:ro" \
      groovy:4.0-jdk17 groovy /check/syntax-check.groovy /jenkins/Jenkinsfile || status=1
else
  echo "  SKIPPED: neither groovy nor docker found"
fi

echo "== jenkins-deployment.yaml renders through the helm chart"
helm repo add jenkins https://charts.jenkins.io > /dev/null
helm repo update > /dev/null
# --namespace and a release name only so the chart's templates have something to interpolate
helm template cassius jenkins/jenkins --namespace default -f "${JENKINS_DIR}/k8s/jenkins-deployment.yaml" > /dev/null \
  && echo "  jenkins-deployment.yaml renders" || status=1

echo "== yaml embedded in jenkins-deployment.yaml"
python3 - "${JENKINS_DIR}/k8s" << 'EOF' || status=1
import sys, yaml
from pathlib import Path

k8s_dir = Path(sys.argv[1])
errors = 0

for path in sorted(k8s_dir.glob("*.yaml")):
    # plain k8s manifests (e.g. docker-cache.yaml) hold several `---`-separated documents;
    # helm values files (jenkins-deployment.yaml) hold one.  safe_load_all handles both.
    try:
        documents = list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
    except yaml.YAMLError as error:
        print(f"  INVALID {path.name}: {error}")
        errors += 1
        continue
    print(f"  {path.name} parses")

    for values in documents:
        if not isinstance(values, dict):
            continue
        # the values in these two maps are themselves yaml documents, and a chart never validates them —
        # a misindented pod template reaches the kubernetes plugin and silently loses its agents
        for keys in (("agent", "podTemplates"), ("controller", "JCasC", "configScripts")):
            embedded = values
            for key in keys:
                embedded = embedded.get(key, {}) if isinstance(embedded, dict) else {}
            for name, document in (embedded or {}).items():
                location = f"{path.name} {'.'.join(keys)}.{name}"
                try:
                    parsed = yaml.safe_load(document)
                except yaml.YAMLError as error:
                    print(f"  INVALID {location}: {error}")
                    errors += 1
                    continue
                print(f"  {location} parses")
                # each pod template may carry a raw kubernetes pod spec in a nested `yaml` key
                for template in parsed if isinstance(parsed, list) else []:
                    if isinstance(template, dict) and "yaml" in template:
                        try:
                            yaml.safe_load(template["yaml"])
                        except yaml.YAMLError as error:
                            print(f"  INVALID {location}.yaml: {error}")
                            errors += 1
                        else:
                            print(f"  {location}.yaml parses")

sys.exit(1 if errors else 0)
EOF

[ 0 -eq ${status} ] && echo "== all .jenkins/ checks passed" || echo "== FAILED"
exit ${status}
