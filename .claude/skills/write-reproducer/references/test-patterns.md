# Test Patterns for Reproducers

Common patterns for writing bug reproducers across different test levels. Adapt to the project's language, test framework, and conventions.

## Contents
1. [Unit tests with fake time](#1-unit-tests-with-fake-time)
2. [Mock/stub tests](#2-mockstub-tests)
3. [Single-instance integration tests](#3-single-instance-integration-tests)
4. [Multi-instance integration tests](#4-multi-instance-integration-tests)
5. [Async waiting helpers](#5-async-waiting-helpers)
6. [Internal/white-box tests](#6-internalwhite-box-tests)

---

## 1. Unit tests with fake time

Use when the bug involves timeouts, expiry, TTLs, retry backoff, or any time the code reads the clock.

**Pattern:** Replace the real clock with a controllable fake. Advance time explicitly to trigger the condition.

```
# Pseudocode — adapt to your language/framework
fake_clock = FakeClock(start_time=0)
component = MyComponent(clock=fake_clock)

component.start()
fake_clock.advance(SESSION_TIMEOUT + 1)

assert component.is_expired()  # Deterministic, no real waiting
```

**Key principle:** If the production code accepts a `Clock`/`Time`/`TimeProvider` interface, inject a fake. If it doesn't, that's a useful finding — the code may need refactoring for testability.

---

## 2. Mock/stub tests

Use when the bug is in component logic that depends on external services, but the bug itself doesn't require a real service.

**When to use mocks:**
- Client-side logic (retry logic, error handling, response parsing)
- Callback/handler behavior
- State machine transitions
- Serialization/deserialization bugs

**Pattern:**
```
# Pseudocode
mock_service = MockService()
client = MyClient(service=mock_service)

# Inject specific responses or errors
mock_service.next_response = ErrorResponse(code=503)

result = client.send(request)

# Assert client-side handling
assert result.was_retried
assert mock_service.call_count == 3  # retried twice
```

**Error injection:** Configure mocks to return specific errors, delays, or malformed responses to trigger the bug's conditions.

---

## 3. Single-instance integration tests

Use when the bug requires a real service/server but not multiple instances.

**Pattern:** Start a single embedded/test instance, run the reproducer against it.

```
# Pseudocode — adapt to your project's test infra
server = start_test_server(config=default_config)

client = create_client(server.address)
client.create_resource("test-resource")
result = client.perform_operation("test-resource", data)

assert result == expected_value

server.stop()
```

**Key considerations:**
- Use the project's existing test fixtures/helpers for starting services
- Prefer embedded/in-process servers over external process management
- Use random/ephemeral ports to avoid conflicts
- Clean up resources in teardown

---

## 4. Multi-instance integration tests

Use when the bug requires replication, leader election, failover, consensus, or cross-instance coordination.

**Pattern:** Start N instances, create resources that span them, then trigger the distributed condition.

```
# Pseudocode
cluster = start_test_cluster(instances=3)

cluster.create_resource("test-resource", replication=3)

# Trigger a distributed event (failover, partition, etc.)
cluster.stop_instance(leader_id)

# Assert correct behavior after the event
result = cluster.read("test-resource")
assert result == expected_value

cluster.stop_all()
```

**Minimize first:** Always try to reproduce with 1 instance before using N. Only increase if the bug requires cross-instance interaction.

---

## 5. Async waiting helpers

Use instead of `sleep()` to wait for asynchronous conditions. Most test frameworks provide these, or they're easy to write.

**Polling pattern:**
```
# Pseudocode
def wait_until(condition, timeout_ms=30000, poll_interval_ms=100, message=""):
    deadline = now() + timeout_ms
    while now() < deadline:
        if condition():
            return
        sleep(poll_interval_ms)
    raise TimeoutError(message)

# Usage
wait_until(
    lambda: client.get_status() == "ready",
    timeout_ms=30000,
    message="Service did not become ready"
)
```

**Retry-on-exception pattern:**
```
# Pseudocode
def retry_until_no_exception(timeout_ms, fn):
    deadline = now() + timeout_ms
    last_error = None
    while now() < deadline:
        try:
            fn()
            return
        except Exception as e:
            last_error = e
            sleep(100)
    raise last_error
```

**Common framework equivalents:**
| Language/Framework | Helper |
|---|---|
| Java/JUnit | `Awaitility.await().atMost(...)`, `TestUtils.waitForCondition()` |
| Python/pytest | `tenacity.retry()`, custom polling loop |
| JavaScript/Jest | `waitFor()`, `jest.advanceTimersByTime()` |
| Go | `require.Eventually()` (testify), `time.After` + select |
| Rust | `tokio::time::timeout()`, custom polling |

---

## 6. Internal/white-box tests

For bugs deep in a component's internals, test the class/function directly without the full service stack.

**When to use:**
- The bug is in a specific algorithm or data structure
- Starting the full service is slow or complex
- You want to test exact internal state transitions

**Pattern:**
```
# Pseudocode — construct the internal component directly
component = InternalComponent(
    config=minimal_config,
    clock=fake_clock,
    dependency=mock_dependency,
)

# Call internal methods directly
component.process(input_data)

# Assert internal state
assert component.internal_state == expected_state
```

**Trade-off:** White-box tests are faster and more precise but couple to internal APIs. If the internal API changes, the test breaks even if the bug is still present. Prefer this only when integration-level testing is too coarse to isolate the trigger.

---

## Discovering project test conventions

Before writing a reproducer, explore the project to understand:

1. **Test framework:** What framework is used? (JUnit, pytest, Jest, Go testing, etc.)
2. **Test layout:** Where do tests live? (`src/test/`, `tests/`, `*_test.go`, `*.spec.ts`, etc.)
3. **Test utilities:** What helpers exist? Look for `testutils/`, `test_helpers/`, `fixtures/`, `conftest.py`, etc.
4. **Test infrastructure:** How are integration tests run? Embedded servers, Docker, test containers, etc.
5. **Naming conventions:** How are tests named? What style do existing tests follow?

Match the project's existing patterns — a reproducer that follows project conventions is easier to review and integrate.
