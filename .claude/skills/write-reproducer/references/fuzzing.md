# Fuzz Testing for Repros

Use a fuzz harness as the repro when the bug involves parsing, deserialization, or processing of untrusted input, or when a crashing input already exists from a fuzzer.

## When to use a fuzz harness as the repro

- A crashing input from a fuzzer already exists — just package it with the harness
- The bug is in a parser, deserializer, codec, or input processor
- The input space is large and a single example misses the point — the harness itself is the repro
- You want the repro to also serve as a regression fuzzer

## When NOT to use a fuzz harness

- The bug requires specific multi-step state setup (use a regular test instead)
- The bug is about business logic, not input processing
- The input structure is too complex for byte-level fuzzing and no structure-aware fuzzer exists

## Frameworks by language

| Language | Framework | Coverage-guided | Structure-aware |
|----------|-----------|----------------|-----------------|
| Rust | cargo-fuzz (libFuzzer) | Yes | Yes (via Arbitrary) |
| Rust | honggfuzz-rs | Yes | No |
| C/C++ | libFuzzer | Yes | No |
| C/C++ | AFL++ | Yes | Via grammar mutators |
| Java | Jazzer | Yes | Yes (via autofuzz + @FuzzTest) |
| Python | Atheris | Yes | No |
| Go | go-fuzz / native `testing.F` | Yes | No |

## Packaging a fuzz repro

A fuzz repro consists of:

1. **The fuzz target**: a function that takes bytes (or a structured input) and feeds them to the code under test
2. **The crashing input**: the specific bytes/file that trigger the bug (stored in a corpus or artifacts directory)
3. **The replay command**: how to run the target with the specific input

### Rust (cargo-fuzz)

```rust
// fuzz/fuzz_targets/parse_input.rs
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // TRIGGER: feed arbitrary bytes to the parser
    // ORACLE: implicit — any panic, crash, or sanitizer violation is a finding
    let _ = my_crate::parse(data);
});
```

Replay:
```bash
cargo +nightly fuzz run parse_input fuzz/artifacts/parse_input/<crash-hash>
```

### Rust with Arbitrary (structured fuzzing)

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
struct Input {
    header: [u8; 4],
    payload: Vec<u8>,
    flags: u8,
}

fuzz_target!(|input: Input| {
    let _ = my_crate::process(&input.header, &input.payload, input.flags);
});
```

### Java (Jazzer)

```java
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;

class ParserFuzzTest {
    @FuzzTest
    void fuzzParser(FuzzedDataProvider data) {
        byte[] input = data.consumeRemainingAsBytes();
        // TRIGGER
        MyParser.parse(input);
        // ORACLE: implicit — Jazzer detects crashes, sanitizer violations,
        // and specific bug detectors (SSRF, path traversal, command injection)
    }
}
```

### Go (native fuzz)

```go
func FuzzParse(f *testing.F) {
    // Seed corpus
    f.Add([]byte("valid input"))
    f.Add([]byte(""))

    f.Fuzz(func(t *testing.T, data []byte) {
        // TRIGGER
        _, _ = Parse(data)
        // ORACLE: implicit — panics are findings
    })
}
```

Replay:
```bash
go test -run FuzzParse/testdata/fuzz/FuzzParse/<crash-file>
```

## Distilling a fuzz finding into a plain test

Sometimes a fuzz finding should be converted into a plain unit test for clarity:

```rust
#[test]
fn issue_xxx_parser_panics_on_truncated_header() {
    // This input was found by cargo-fuzz (seed: <hash>)
    // It triggers a panic in the header length check
    let input = b"\x00\x01";  // truncated header (needs 4 bytes)

    // ORACLE: should return Err, not panic
    let result = std::panic::catch_unwind(|| my_crate::parse(input));
    assert!(result.is_ok(), "parser panicked on truncated input");
}
```

Do this when:
- The crashing input is small and the bug is clear
- You want the test in the regular test suite, not in the fuzz directory
- The fuzz infrastructure isn't available in CI

Keep the fuzz harness when:
- The input is large or opaque
- You want continued fuzzing to find related bugs
- The crashing input is one of many interesting nearby inputs

## Test-case reduction for fuzz findings

When the crashing input is large, reduce it:

- **cargo-fuzz**: `cargo +nightly fuzz tmin <target> <crash-file>`
- **libFuzzer**: built-in minimizer via `-minimize_crash=1`
- **AFL++**: `afl-tmin`
- **For structured inputs**: use Perses (grammar-aware) or C-Reduce (C/C++ specific)

Always verify the reduced input triggers the *same* crash (same stack trace, same sanitizer report).
