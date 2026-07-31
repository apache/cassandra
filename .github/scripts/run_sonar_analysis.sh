#!/bin/bash
set +e

# Get Git/GitHub context
GIT_BRANCH="${GITHUB_HEAD_REF:-$GITHUB_REF_NAME}"
GIT_REPO_URL="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY"
GIT_BASE_BRANCH="${GITHUB_BASE_REF:-main}"

# SonarQube configuration (from job env vars)
PROJECT_KEY="${SONAR_PROJECT_KEY}"
PROJECT_NAME="${SONAR_PROJECT_NAME}"
SONAR_HOST_URL="${SONAR_HOST}"

# Retry configuration
MAX_RETRIES=3
RETRY_DELAY=30

echo "=========================================="
echo "SonarQube Analysis Configuration"
echo "=========================================="
echo "Project Key: $PROJECT_KEY"
echo "Project Name: $PROJECT_NAME"
echo "Branch: $GIT_BRANCH"
echo "SonarQube Host: $SONAR_HOST_URL"
echo "Max Retries: $MAX_RETRIES"
echo "=========================================="

# Note: Authentication uses the SONAR_TOKEN environment variable.
# This is the standard SonarQube practice - sonar-scanner automatically 
# reads SONAR_TOKEN from the environment.
#
# Note: No truststore configuration needed. The whitewater.ibm.com endpoint 
# uses Cloudflare with public DigiCert certificates trusted by system CA bundles.

# Build sonar-scanner arguments
SONAR_ARGS=(
  -Dsonar.projectKey="$PROJECT_KEY"
  -Dsonar.projectName="$PROJECT_NAME"
  -Dsonar.host.url="$SONAR_HOST_URL"
  -Dsonar.token="$SONAR_TOKEN"
  -Dsonar.links.homepage="$GIT_REPO_URL"
  -Dsonar.qualitygate.wait=true
)

# Add PR-specific or branch-specific arguments
if [[ "$GITHUB_EVENT_NAME" == "pull_request" ]]; then
  GIT_SHA="${GITHUB_EVENT_PULL_REQUEST_HEAD_SHA}"
  GIT_PR="${GITHUB_EVENT_PULL_REQUEST_NUMBER}"
  
  SONAR_ARGS+=(
    -Dsonar.pullrequest.key="$GIT_PR"
    -Dsonar.pullrequest.branch="$GIT_BRANCH"
    -Dsonar.pullrequest.base="$GIT_BASE_BRANCH"
    -Dsonar.scm.revision="$GIT_SHA"
    -Dsonar.links.scm="$GIT_REPO_URL/pull/$GIT_PR"
  )
else
  SONAR_ARGS+=(
    -Dsonar.branch.name="$GIT_BRANCH"
    -Dsonar.links.scm="$GIT_REPO_URL/tree/$GIT_BRANCH"
  )
fi

# Add debug flag if enabled
if [[ "$DEBUG_MODE" == "true" ]]; then
  echo "Debug mode enabled: Sonar verbose output"
  SONAR_ARGS+=(-Dsonar.verbose=true)
fi

# Retry loop
for attempt in $(seq 1 $MAX_RETRIES); do
  echo ""
  echo "=========================================="
  echo "Attempt $attempt of $MAX_RETRIES"
  echo "=========================================="
  
  if [[ $attempt -gt 1 ]]; then
    echo "Waiting ${RETRY_DELAY}s before retry..."
    sleep $RETRY_DELAY
    RETRY_DELAY=$((RETRY_DELAY * 2))
  fi
  
  echo "Starting SonarQube analysis..."
  set -x
  sonar-scanner "${SONAR_ARGS[@]}" 2>&1 | tee sonar-output.log
  RESULT=$?
  set +x

  # Check for Quality Gate failure first (this is NOT an error to retry)
  if grep -q "QUALITY GATE STATUS: FAILED" sonar-output.log; then
    echo ""
    echo "=========================================="
    echo "⚠️ Quality Gate FAILED"
    echo "=========================================="
    echo "Dashboard: $SONAR_HOST_URL/dashboard?id=$PROJECT_KEY&branch=$GIT_BRANCH"
    echo "This is a code quality issue - fix the issues reported and re-run."
    echo "result=quality_gate_failed" >> $GITHUB_OUTPUT
    exit 1
  fi

  # Check for errors in log even if exit code is 0 (sonar-scanner bug in newer versions)
  if grep -Eq "ERROR|FAILURE|BUILD FAILURE|Failed to|IllegalStateException|EXECUTION FAILURE" sonar-output.log; then
    echo "ERROR detected in scanner output, treating as failure"
    RESULT=1
  fi

  if [[ $RESULT -eq 0 ]]; then
    echo ""
    echo "=========================================="
    echo "✓ SonarQube analysis completed successfully!"
    echo "=========================================="

    # Extract dashboard URL from report-task.txt (like Jenkins does)
    if [[ -f ".scannerwork/report-task.txt" ]]; then
      DASHBOARD_URL=$(grep "^dashboardUrl=" .scannerwork/report-task.txt | cut -d'=' -f2-)
      if [[ -n "$DASHBOARD_URL" ]]; then
        echo "Dashboard URL: $DASHBOARD_URL"
        echo "dashboard_url=$DASHBOARD_URL" >> $GITHUB_OUTPUT
      fi
    fi

    echo "result=success" >> $GITHUB_OUTPUT
    exit 0
  else
    echo ""
    echo "=========================================="
    echo "✗ SonarQube analysis failed with exit code: $RESULT"
    echo "=========================================="

    # Check if this is a transient error (retry these)
    if grep -Eqi "503|Service Unavailable|Timeout|Connection reset|temporarily unavailable|ConnectException|SocketTimeoutException" sonar-output.log; then
      if [[ $attempt -lt $MAX_RETRIES ]]; then
        echo "Transient error detected. Will retry..."
      else
        echo "Max retries reached. Failing."
        echo "result=failure" >> $GITHUB_OUTPUT
        exit $RESULT
      fi
    else
      # Other errors - don't retry
      echo "Non-transient error. Not retrying."
      echo "result=failure" >> $GITHUB_OUTPUT
      exit $RESULT
    fi
  fi
done
