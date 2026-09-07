#!/usr/bin/env python3
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
"""
Driver for AccordNotify.tla: runs TLC over (shape x profile) and tabulates the G1-G4
guarantees plus the coverage probes.

Each probe is a NEGATED reachability claim, so TLC reporting it violated means the
situation IS reached.  They cannot be checked together - TLC halts at the first violated
invariant - which is why they need a driver rather than a line in Notify.cfg.

The probe expectations are part of the contract, in both directions.  Probe_Nested is
expected UNREACHABLE (no handler mutates a queue position any more, so delivery is depth
1 - L3_HandlerTakesNoPosition is the property form of the same claim), and so is
Probe_UpgradeWouldDoubleFile under the production profiles - it is a documented GAP rather than
a design property, and is reached only under ctl-no-revoke-notify (DOUBLE_FILE_REACHED); every
other probe must be reached in every shape that can build its situation (see
expected_unreached), or the guarantee it underwrites was checked over a state space that never
built the situation.  main() exits non-zero on any deviation.

Usage:
    ./notify.py --list
    ./notify.py --shapes mixed --profiles baseline
    ./notify.py                          # everything
"""
import argparse
import concurrent.futures
import os
import re
import shutil
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = os.path.join(HERE, "AccordNotify.tla")

# G1_BatchLed is the model-side statement of RELEASE_QUEUE's REQUIRE_RUNNABLE for a non-sync
# task's keys: NonSyncState.prepareExclusive no longer re-checks the batch, so everything it
# captured must still be led at prepare time.  G1_Strong follows from it and is required too;
# both are checked as their own columns so a failure names the weaker or the stronger claim.
INVARIANTS = [
    "TypeOK",
    "G4_Bounded",
    "G1_SyncSound",
    "G1_PositionsHeld",
    "G1_BatchLed",
    "G1_Strong",
    "G3_Disjoint",
    "G2_NoLostWakeup",
]

PROPERTIES = ["G4_Drains", "Termination", "L3_HandlerTakesNoPosition"]

PROBES = [
    "Probe_Nested",
    "Probe_Contention",
    "Probe_Runnable",
    "Probe_KeptKeys",
    "Probe_KeyNoteWhileWaitTxn",
    "Probe_StaleBatch",
    "Probe_EmptyBatch",
    "Probe_Upgraded",
    "Probe_UpgradeDisplaces",
    "Probe_UpgradeWouldDoubleFile",
]

# Probe_Nested is expected unreachable; see AccordNotify.tla where it is defined.
# Probe_UpgradeWouldDoubleFile too, and that one is a GAP rather than a design property: it is
# the precondition under which onKeyMovedToFifo's inline fold could file one key into both
# batch sets, so G3_Disjoint is checked over a state space that never builds it.
EXPECT_UNREACHED = {"Probe_Nested", "Probe_UpgradeWouldDoubleFile"}

SHORT = {
    "TypeOK": "Typ", "G4_Bounded": "G4b", "G1_SyncSound": "G1s",
    "G1_PositionsHeld": "G1p", "G1_BatchLed": "G1b", "G1_Strong": "G1S",
    "G3_Disjoint": "G3",
    "G2_NoLostWakeup": "G2", "G4_Drains": "G4d", "Termination": "Trm",
    "L3_HandlerTakesNoPosition": "L3",
}

PSHORT = {
    "Probe_Nested": "nest", "Probe_Contention": "cont", "Probe_Runnable": "runn",
    "Probe_KeptKeys": "keptK", "Probe_KeyNoteWhileWaitTxn": "keyNote",
    "Probe_StaleBatch": "stale", "Probe_EmptyBatch": "empty",
    "Probe_Upgraded": "upg", "Probe_UpgradeDisplaces": "upgQ",
    "Probe_UpgradeWouldDoubleFile": "dblFile",
}

# ---------------------------------------------------------------------------
# Shapes.  regions/sync are per task; a FIFO task models one that has been stamped.
# ---------------------------------------------------------------------------
SHAPES = {
    # the checked-in example: enough for a task to be demoted on the txnId while holding
    # key positions, which is the state the two waitingFor counters must survive
    "mixed": dict(
        cmd=["c1"], key=["k1", "k2"],
        txns=[["c1"], ["c1"], []],
        keys=[["k1", "k2"], ["k1"], ["k1", "k2"]],
        region=["ORD", "FIFO", "ORD"],
        sync=[True, False, False]),

    # every task non-sync, so the batch sets carry all the key accounting and the
    # prepareExclusive re-check is on the critical path for all of them
    "all-nonsync": dict(
        cmd=["c1"], key=["k1", "k2"],
        txns=[["c1"], ["c1"], []],
        keys=[["k1", "k2"], ["k1", "k2"], ["k1"]],
        region=["ORD", "ORD", "ORD"],
        sync=[False, False, False]),

    # every task SYNC, so wkey carries it instead and G1_SyncSound is the binding
    # guarantee everywhere (REQUIRE_RUNNABLE would trip on a violation)
    "all-sync": dict(
        cmd=["c1"], key=["k1", "k2"],
        txns=[["c1"], ["c1"], []],
        keys=[["k1", "k2"], ["k1"], ["k1", "k2"]],
        region=["ORD", "ORD", "ORD"],
        sync=[True, True, True]),

    # a bag member behind two sequenced tasks: the bag is the last region, so it is
    # notified only once nothing sequenced is queued
    "with-bag": dict(
        cmd=["c1"], key=["k1", "k2"],
        txns=[["c1"], ["c1"], []],
        keys=[["k1", "k2"], ["k1"], ["k1", "k2"]],
        region=["FIFO", "ORD", "BAG"],
        sync=[False, False, False]),
}

PROFILES = {
    # MinBatch 1 and an inert threshold are the two real regimes, as in matrix.py
    "baseline":        dict(MinBatch=1, MaxDepth=4, PDrainNotifies=True,
                           PRevokeNotifies=True, PModelLoading=False),
    "inert-threshold": dict(MinBatch=9, MaxDepth=4, PDrainNotifies=True,
                            PRevokeNotifies=True, PModelLoading=False),
    "loading":         dict(MinBatch=1, MaxDepth=4, PDrainNotifies=True,
                            PRevokeNotifies=True, PModelLoading=True),
    # negative control: the drain must make up the whole delta for a loading entry, since
    # nothing on one was ever notified.  MUST break G2.
    "ctl-no-drain-notify": dict(MinBatch=1, MaxDepth=4, PDrainNotifies=False,
                                PRevokeNotifies=True, PModelLoading=True),
    # negative control for G1: losing the prefix is no longer delivered, so a key stays in
    # the batch sets after the task stops leading it - and prepareExclusive, which no longer
    # re-checks, would lock it and trip RELEASE_QUEUE's REQUIRE_RUNNABLE.  MUST break
    # G1_BatchLed; without it that invariant could not fail in any profile.
    "ctl-no-revoke-notify": dict(MinBatch=1, MaxDepth=4, PDrainNotifies=True,
                                 PRevokeNotifies=False, PModelLoading=False),
}

CONTROLS = {"ctl-no-drain-notify", "ctl-no-revoke-notify"}

# Shapes containing at least one non-sync task.  A SYNC task awaits every load it needs
# (MustAwaitLoad covers its keys as well as its txnIds) and re-claims in OnLoaded, so it
# never depends on the drain's notification; only a non-sync task takes key positions
# while the key entry is still loading, and so only it can lose that wakeup.
NONSYNC_SHAPES = {s for s, v in SHAPES.items() if not all(v["sync"])}

# ...and shapes containing at least one SYNC task, whose readiness is carried by the wkey
# counter rather than by the batch sets: only there can an undelivered revocation leave a
# SYNC task believing it is runnable (G1_SyncSound).
SYNC_SHAPES = {s for s, v in SHAPES.items() if any(v["sync"])}

# {profile: {invariant/property: {witness shapes}}}; a profile with no entry must be green.
# A lost wakeup both breaks G2 and strands the task, so Termination goes too.
EXPECT_FAIL = {
    "ctl-no-drain-notify": {
        "G2_NoLostWakeup": NONSYNC_SHAPES,
        "Termination": NONSYNC_SHAPES,
    },
    # An undelivered revocation is not a lost wakeup - nothing is stranded, so G2 and
    # Termination survive - it is a stale BELIEF, which is what the removed re-check used to
    # absorb.  It breaks soundness three ways: the batch sets keep a key the task no longer
    # leads (G1_BatchLed) and so the task believes it can run (G1_Strong); a SYNC task's wkey
    # counter is never incremented back (G1_SyncSound); and a later NEWLY_BLOCKING_RUNNABLE
    # files a key into blocking that is still in notBlocking, since onNewBlockingHead only
    # asserts the other set is clear rather than moving the key (G3_Disjoint).
    "ctl-no-revoke-notify": {
        "G1_BatchLed": NONSYNC_SHAPES,
        "G1_Strong": set(SHAPES),
        "G1_SyncSound": SYNC_SHAPES,
        "G3_Disjoint": NONSYNC_SHAPES,
    },
}

# (profile, shape) cells in which Probe_UpgradeWouldDoubleFile IS reachable.  It is a
# documented gap rather than a design property (see AccordNotify.tla), and under
# ctl-no-revoke-notify the stale batch it needs survives to quiescence, so the fold's
# dangerous case is built - which is where G3_Disjoint reports it.
DOUBLE_FILE_REACHED = {("ctl-no-revoke-notify", "mixed"),
                       ("ctl-no-revoke-notify", "all-nonsync")}


def expected_failures(prof_name, shape_name):
    exp = EXPECT_FAIL.get(prof_name, {})
    return {c for c, shapes in exp.items() if shape_name in shapes}


def expected_unreached(shape_name, prof_name):
    """Probe_Nested is unreachable by design - no handler mutates a queue position, so a
    notification cannot generate another (L3_HandlerTakesNoPosition is the property form of
    the same claim).  The two batch-transient probes both test ~TaskSync[t], so they cannot fire
    in an all-SYNC shape, and the two upgrade probes need a non-sync ORD task, since only such
    a task can moveToFifo; derived rather than tabulated so the two cannot drift apart."""
    out = set(EXPECT_UNREACHED)
    if (prof_name, shape_name) in DOUBLE_FILE_REACHED:
        out.discard("Probe_UpgradeWouldDoubleFile")
    if all(SHAPES[shape_name]["sync"]):
        out |= {"Probe_StaleBatch", "Probe_EmptyBatch"}
    if not any(r == "ORD" and not s
               for r, s in zip(SHAPES[shape_name]["region"], SHAPES[shape_name]["sync"])):
        out |= {"Probe_Upgraded", "Probe_UpgradeDisplaces"}
    return out


def tla_set(xs):
    return "{" + ", ".join('"%s"' % x for x in xs) + "}"


def tla_seq_of_sets(xss):
    return "<<" + ", ".join(tla_set(xs) for xs in xss) + ">>"


def gen(dirname, name, shape, policy):
    n = len(shape["txns"])
    with open(os.path.join(dirname, name + ".tla"), "w") as f:
        f.write("---- MODULE %s ----\n" % name)
        f.write("EXTENDS AccordNotify\n")
        f.write("MCTaskTxns == %s\n" % tla_seq_of_sets(shape["txns"]))
        f.write("MCTaskKeys == %s\n" % tla_seq_of_sets(shape["keys"]))
        f.write("MCTaskRegion == <<%s>>\n"
                % ", ".join('"%s"' % r for r in shape["region"]))
        f.write("MCTaskSync == <<%s>>\n"
                % ", ".join(str(b).upper() for b in shape["sync"]))
        f.write("====\n")
    with open(os.path.join(dirname, name + ".cfg"), "w") as f:
        f.write("SPECIFICATION Spec\n")
        # OFF, as in the checked-in Notify.cfg: every task reaching Done is a legitimate
        # terminal state, which TLC would report as a deadlock.  A wedged cascade is
        # caught instead by G4_Bounded (Deliver is guarded one level above it, so hitting
        # the bound is a reported violation) and by G4_Drains / Termination below.
        f.write("CHECK_DEADLOCK FALSE\n")
        f.write("CONSTANTS\n")
        f.write("  CmdEntries = %s\n" % tla_set(shape["cmd"]))
        f.write("  KeyEntries = %s\n" % tla_set(shape["key"]))
        f.write("  NumTasks = %d\n" % n)
        f.write("  TaskTxns <- MCTaskTxns\n")
        f.write("  TaskKeys <- MCTaskKeys\n")
        f.write("  TaskRegion <- MCTaskRegion\n")
        f.write("  TaskSync <- MCTaskSync\n")
        for k, v in policy.items():
            f.write("  %s = %s\n" % (k, str(v).upper() if isinstance(v, bool) else v))


VIOLATION = re.compile(r"Invariant (\w+) is violated")


def completed_cleanly(out):
    if "Model checking completed" not in out:
        bad = [l for l in out.splitlines() if l.startswith("Error:")]
        return bad[0][:90] if bad else "DID NOT COMPLETE"
    if "Error:" in out:
        bad = [l for l in out.splitlines() if l.startswith("Error:")]
        return bad[0][:90]
    return None


def write_checks(path, lines):
    with open(path) as f:
        cfg = [l for l in f if not l.startswith(("INVARIANT", "PROPERTY"))]
    with open(path, "w") as f:
        f.writelines(cfg)
        f.writelines(lines)


def run_one(shape_name, prof_name, timeout):
    shape, policy = SHAPES[shape_name], PROFILES[prof_name]
    d = tempfile.mkdtemp(prefix="accordnotify-")
    try:
        shutil.copy(SPEC, d)
        mod = "N"
        gen(d, mod, shape, policy)
        cfg = os.path.join(d, mod + ".cfg")
        cmd = ["tlc", "-workers", "2", "-cleanup", "-config", mod + ".cfg", mod + ".tla"]

        def tlc(lines):
            write_checks(cfg, lines)
            try:
                return subprocess.run(cmd, cwd=d, capture_output=True, text=True,
                                      timeout=timeout).stdout
            except subprocess.TimeoutExpired:
                return None

        failed, states = [], 0
        # invariants first, re-running past each violation so all of them are reported
        for _ in range(len(INVARIANTS) + 1):
            remaining = [i for i in INVARIANTS if i not in failed]
            if not remaining:
                break
            out = tlc(["INVARIANT %s\n" % i for i in remaining])
            if out is None:
                return dict(shape=shape_name, profile=prof_name, failed=failed,
                            error="TIMEOUT", states=states, probes={})
            m = re.search(r"(\d+) states generated, (\d+) distinct", out)
            if m:
                states = max(states, int(m.group(2)))
            v = VIOLATION.search(out)
            if v:
                failed.append(v.group(1))
                continue
            bad = completed_cleanly(out)
            if bad:
                return dict(shape=shape_name, profile=prof_name, failed=failed,
                            error=bad, states=states, probes={})
            break
        # temporal properties, one at a time (they are the reason Spec carries fairness)
        for p in PROPERTIES:
            out = tlc(["PROPERTY %s\n" % p])
            if out is None:
                return dict(shape=shape_name, profile=prof_name, failed=failed,
                            error="TIMEOUT", states=states, probes={})
            if "Temporal properties were violated" in out:
                failed.append(p)
            elif completed_cleanly(out):
                return dict(shape=shape_name, profile=prof_name, failed=failed,
                            error=completed_cleanly(out), states=states, probes={})
        # probes: alone, and "violated" means reached
        probes = {}
        for pr in PROBES:
            out = tlc(["INVARIANT %s\n" % pr])
            if out is None:
                probes[pr] = "unknown"
            elif "is violated" in out:
                probes[pr] = "reached"
            elif completed_cleanly(out) is None:
                probes[pr] = "unreached"
            else:
                probes[pr] = "unknown"
        return dict(shape=shape_name, profile=prof_name, failed=failed,
                    error=None, states=states, probes=probes)
    finally:
        shutil.rmtree(d, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shapes", nargs="*", default=list(SHAPES))
    ap.add_argument("--profiles", nargs="*", default=list(PROFILES))
    ap.add_argument("--timeout", type=int, default=900)
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--list", action="store_true")
    a = ap.parse_args()

    if a.list:
        print("shapes:  ", " ".join(SHAPES))
        print("profiles:", " ".join(PROFILES))
        return 0

    checks = INVARIANTS + PROPERTIES
    jobs = [(s, p) for p in a.profiles for s in a.shapes]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.jobs) as ex:
        futs = {ex.submit(run_one, s, p, a.timeout): (s, p) for s, p in jobs}
        for fut in concurrent.futures.as_completed(futs):
            results[futs[fut]] = fut.result()
            print(".", end="", flush=True)
    print()

    hdr = "%-22s %-16s %s | probes reached" % ("profile", "shape", " ".join(
        "%-4s" % SHORT[c] for c in checks))
    print(hdr)
    print("-" * len(hdr))
    deviations, worst = [], 0
    for p in a.profiles:
        broke = False
        for s in a.shapes:
            r = results[(s, p)]
            unchecked = r["error"] is not None
            cells = ["%-4s" % ("FAIL" if c in r["failed"]
                               else ("?" if unchecked else "ok")) for c in checks]
            probes = r.get("probes", {})
            reached = [x for x in PROBES if probes.get(x) == "reached"]
            unreached = [x for x in PROBES if probes.get(x) == "unreached"]
            unknown = [x for x in PROBES if probes.get(x) == "unknown"]
            cov = " ".join(PSHORT[x] for x in reached)
            if unreached:
                cov += "   (unreached: %s)" % ",".join(PSHORT[x] for x in unreached)
            if unknown:
                cov += "   (UNKNOWN: %s)" % ",".join(PSHORT[x] for x in unknown)
            note = ""
            if r["error"]:
                note = "  <<%s>>" % r["error"]
                worst = 2
                deviations.append("%s/%s did not complete: %s" % (p, s, r["error"]))
            else:
                got, want = set(r["failed"]), expected_failures(p, s)
                if got - want:
                    deviations.append("%s/%s broke unexpectedly: %s"
                                      % (p, s, ", ".join(sorted(got - want))))
                if want - got:
                    deviations.append("%s/%s no longer breaks %s - control may be vacuous"
                                      % (p, s, ", ".join(sorted(want - got))))
                if got:
                    broke = True
                # probe expectations, both directions
                unreachable = expected_unreached(s, p)
                for x in PROBES:
                    st = probes.get(x)
                    if st == "unknown":
                        deviations.append("%s/%s probe %s inconclusive" % (p, s, x))
                    elif x in unreachable and st == "reached":
                        deviations.append(
                            "%s/%s reached %s, which is expected UNREACHABLE here"
                            % (p, s, x))
                    elif x not in unreachable and st == "unreached":
                        deviations.append(
                            "%s/%s never reached %s, so the guarantee it underwrites was "
                            "checked over a state space without that situation"
                            % (p, s, x))
            print("%-22s %-16s %s | %s%s" % (p, s, " ".join(cells), cov, note))
        if p in CONTROLS and not broke:
            deviations.append("%s broke NOTHING - the negative control is vacuous" % p)
        print()

    if deviations:
        worst = max(worst, 1)
        print("DEVIATIONS (%d):" % len(deviations))
        for d in deviations:
            print("  ! " + d)
    else:
        print("all guarantees hold, all probes as expected, every control broke its property.")
    return worst


if __name__ == "__main__":
    sys.exit(main())
