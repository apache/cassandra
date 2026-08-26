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
Driver for AccordExec.tla: runs TLC over (topology x profile) and tabulates which
invariants hold, plus which situations each cell actually reached.

Reads a row as a pair: the invariants, and the coverage.  An unreached probe means
an unchecked row, not a passing one.  A cell that did not complete is reported as
`?`, never as `ok` - absence of a violation is not success.

EXIT STATUS IS PART OF THE CONTRACT.  Every `ctl-*` profile must break the property it
was built to break: a control that has silently become vacuous is the failure mode the
whole scheme guards against, so EXPECT_FAIL below records what each cell must do and
main() exits non-zero on any deviation - an unexpected failure, a missing expected
failure, or a cell that did not complete.

Usage:
    ./matrix.py --list
    ./matrix.py --profiles baseline ctl-defer-submit --topologies keys-only
    ./matrix.py --no-probes          # faster, but a green table may be vacuous
    ./matrix.py --liveness           # also check Termination (temporal, slow)

EXPECT_FAIL is a table over the INVARIANTS.  --liveness adds Termination, which is checked in
a pass of its own after the invariant loop (TLC checks liveness during the search and stops at
the first violation, so checking it alongside would leave the safety search incomplete while
the remaining invariants still printed `ok`).  It is deliberately not in EXPECT_FAIL: a control
that breaks NoStuck also strands tasks, so Termination breaks there too and is reported as an
unexpected break - read that row as "expected, but not tabulated", and run --liveness on the
baseline profiles when you want a clean exit.

RUNTIME.  Most cells are seconds; the outlier is baseline-full/non-leading-waiter at ~13
minutes on a dedicated core.  The full 15 x 8 matrix with probes is about 55 minutes at
--jobs 5 on 5 cores.  A timeout is an ERROR, not a `?` to be shrugged at, so the default is
generous: if you oversubscribe - TLC runs 2 workers per cell, so --jobs above the core count
does - raise it rather than lowering it.
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
SPEC = os.path.join(HERE, "AccordExec.tla")

# every invariant we ask TLC about, in reporting order
INVARIANTS = [
    "TypeOK",
    "Inv_LockerIsFifo",
    "Inv_LockLeads",
    "Inv_OneProspectiveLocker",
    "Inv_AtMostOneLock",
    "Inv_Isolation",
    "RankOK",
    "NoCycle",
    "NoStuck",
]

# temporal properties, checked only with --liveness (they need the fairness conjunct in
# Spec, and cost noticeably more than the invariants)
PROPERTIES = ["Termination"]

# Coverage probes.  Each is a NEGATED reachability claim, so TLC reporting it as
# "violated" means the situation IS reached.  A safety table over a model that
# never reaches a held lock or a threshold wait proves nothing, so these are
# reported alongside and a missing one is flagged.
PROBES = [
    "Probe_Contention",
    "Probe_AnyWait",
    "Probe_LockHeld",
    "Probe_LockHasWaiter",
    "Probe_LockHasFifoWaiter",
    "Probe_BetweenRounds",
    "Probe_ThresholdWait",
    "Probe_BlockedOnTxnHoldingKeys",
    "Probe_Consequence",
    "Probe_TopLevelAtomic",
    "Probe_UnitRevisits",
]

# ---------------------------------------------------------------------------
# Topologies.  cmd/key name the entries; txns/keys/parent are per task.
#
# ATOMIC is modelled only for a consequence, so a topology exercising it needs a
# parent relation.  Two tasks sharing a txnId contend on it; they still hold their
# key positions while blocked on it, so key contention does not need disjoint txnIds.
# ---------------------------------------------------------------------------
TOPOLOGIES = {
    # two independent tasks sharing one txnId and both keys: the smallest topology in
    # which two prospective HOLD_QUEUE lockers can meet, and so the witness that
    # ctl-unseq-incr-txn needs.  Two keys, so an INCR task can take a partial batch and
    # hold its txnId lock between rounds
    "two-lockers": dict(
        cmd=["c1"], key=["k1", "k2"],
        txns=[["c1"], ["c1"]], keys=[["k1", "k2"], ["k1", "k2"]],
        parent=[0, 0]),

    # a holder that keeps a txnId across rounds, against a consequence that keeps its
    # key position while blocked on that txnId
    "hold-vs-consequence": dict(
        cmd=["c1"], key=["k1", "k2"],
        txns=[["c1"], ["c1"]], keys=[["k1", "k2"], ["k1"]],
        parent=[0, 1]),

    # two lock holders with disjoint txnIds contending for the same keys, so both can
    # be running at once
    "disjoint-txns": dict(
        cmd=["c1", "c2"], key=["k1", "k2"],
        txns=[["c1"], ["c2"]], keys=[["k1", "k2"], ["k1", "k2"]],
        parent=[0, 0]),

    # the middle task leads the entry it shares with the third, so the third blocks
    # without ever being the first blocked member anywhere
    "non-leading-waiter": dict(
        cmd=["c1"], key=["k1", "k2", "k3"],
        txns=[["c1"], ["c1"], []],
        keys=[["k1", "k2", "k3"], ["k2", "k3"], ["k1", "k2"]],
        parent=[0, 0, 0]),

    # a lock holder against tasks that declare no txnId, so nothing serialises them
    "keys-only": dict(
        cmd=["c1"], key=["k1", "k2"],
        txns=[["c1"], [], []],
        keys=[["k1", "k2"], ["k1", "k2"], ["k1"]],
        parent=[0, 0, 0]),

    # a consequence whose keys extend beyond its submitter's - the only case
    # alwaysReady applies to - plus a task contending for the added key
    "consequence-non-subset": dict(
        cmd=["c1"], key=["k1", "k2", "k3"],
        txns=[["c1"], ["c1"], []],
        keys=[["k1", "k2"], ["k1", "k2", "k3"], ["k2", "k3"]],
        parent=[0, 1, 0]),

    # a three-link chain (2 -> 3 -> 4) against a foreign task that sorts first, so a
    # link whose child is not ATOMIC is exposed: the unit ends there, and the foreign
    # task may run between the two sides of it.  The only topology deeper than one
    # submission, which is what makes the atomic-unit boundary observable
    "deep-chain": dict(
        cmd=["c1"], key=["k1"],
        txns=[[], [], [], []],
        keys=[["k1"], ["k1"], ["k1"], ["k1"]],
        parent=[0, 0, 2, 3]),

    # the only topology in which a task declares BOTH context.primaryTxnId() and
    # context.additionalTxnId().  Without it LeadsAllTxns/TxnReady are never a real
    # conjunction, and O10's asserted txnId-subset restriction on an ATOMIC consequence is
    # only ever {} \subseteq X or X \subseteq X - here task 2 declares {c1} \subset
    # {c1,c2}, a proper non-trivial subset.  Two keys, so an INCR task can take a partial
    # batch and hold both txnId locks between rounds
    "two-txns": dict(
        cmd=["c1", "c2"], key=["k1", "k2"],
        txns=[["c1", "c2"], ["c1"], ["c2"]],
        keys=[["k1", "k2"], ["k1", "k2"], ["k1"]],
        parent=[0, 1, 0]),
}

# ---------------------------------------------------------------------------
# Profiles.  MinBatch is 16 in production, larger than any key set at model scale,
# which makes thresholds inert and every non-sync task behave as SYNC; MinBatch=1 is
# the other real regime (and what alwaysReady produces).  Both are exercised.
# ---------------------------------------------------------------------------
BASE = dict(
    MinBatch=1, BlockedLimit=0,
    PAlwaysReady='"ON_NON_SUBSET"',
    PModelLoading=False,
    PAllowAdoption=False,
    PPartialRounds=True,
    # controls, each disabling one implementation assertion
    PUpgradeOnStart=True,
    PSubmitBeforeRelease=True,
    PAllowUnseqIncrWithTxn=False,
    PAllowFifoAdoption=False,
    PAllowDoubleLock=False,
    # semantics probe, not a control
    PBagInterleaves=False,
)


def prof(**over):
    p = dict(BASE)
    p.update(over)
    return p


PROFILES = {
    # ---- the implementation ----------------------------------------------
    "baseline":            prof(),
    "baseline-load":       prof(PModelLoading=True),
    "baseline-adopt":      prof(PAllowAdoption=True),
    "baseline-blocked":    prof(BlockedLimit=1),
    "baseline-inert-thr":  prof(MinBatch=9, PPartialRounds=False),
    "baseline-full":       prof(PModelLoading=True, PAllowAdoption=True,
                                BlockedLimit=1),

    # ---- alwaysReady variants -------------------------------------------
    "ar-first-run":        prof(PAlwaysReady='"FIRST_RUN"'),
    "ar-always":           prof(PAlwaysReady='"ALWAYS"'),
    "ar-never":            prof(PAlwaysReady='"NEVER"', MinBatch=9),

    # ---- controls: each MUST break something -----------------------------
    "ctl-no-upgrade":      prof(PUpgradeOnStart=False),
    "ctl-defer-submit":    prof(PSubmitBeforeRelease=False),
    "ctl-unseq-incr-txn":  prof(PAllowUnseqIncrWithTxn=True),
    # O5: relaxing addCachedKeyExclusive's !isCacheQueuedFifo guard is sound on
    # ordering grounds - a claim taken outside the acquisition pass is placed by its
    # key, not by arrival - but it breaks isolation, which is what the guard protects.
    "ctl-fifo-adopt":      prof(PAllowAdoption=True, PAllowFifoAdoption=True),
    # A3: CanRun conjoins NoForeignLock, i.e. it assumes lockExclusive's
    # require(!isLocked()) rather than deriving it, so Inv_AtMostOneLock cannot fail
    # without this control - which is exactly the "green over a model that never reaches
    # the situation" failure mode the scheme is built to expose.  PUpgradeOnStart goes
    # with it because addFifo pins a fifo holder at the head: while the holder leads by
    # construction, no second task can reach the lock, so the guard's precondition is
    # unreachable until the upgrade is gone too.  See EXPECT_FAIL.
    "ctl-double-lock":     prof(PUpgradeOnStart=False, PAllowDoubleLock=True),
    # Not a control: it admits an interleaving bag - the reading in which an UNSEQUENCED task
    # may run inside an ATOMIC unit - and it must break RankOK and NoCycle, which is why
    # nothing in the implementation permits it (nor does the ATOMIC javadoc ask for it: it
    # promises atomicity "with respect to other tasks").  See README.
    "probe-bag-interleaves": prof(PBagInterleaves=True),
}

# Profiles that disable an implementation assertion, plus the semantics probe.  Each MUST
# break at least one property somewhere in the run: a control that breaks nothing has
# become vacuous and is no longer evidence for anything.
CONTROLS = {"ctl-no-upgrade", "ctl-defer-submit", "ctl-unseq-incr-txn", "ctl-fifo-adopt",
            "ctl-double-lock", "probe-bag-interleaves"}

# What each cell MUST do, as {profile: {invariant: {witness topologies}}}.  A profile with
# no entry must be entirely green.  Deviation in either direction is an error: an
# unexpected failure is a regression, and a missing one means the control no longer
# reaches the situation it was built for (controls are topology-sensitive, so the witness
# set is named rather than assumed to be every topology).
EXPECT_FAIL = {
    "ctl-no-upgrade": {
        # vacuous on deep-chain alone, which declares no txnId, so HoldsAcrossRuns is
        # false for every task and both invariants have no instances there
        "Inv_LockerIsFifo": {"two-lockers", "hold-vs-consequence", "disjoint-txns",
                             "non-leading-waiter", "keys-only",
                             "consequence-non-subset", "two-txns"},
        "Inv_LockLeads": {"two-lockers", "hold-vs-consequence", "disjoint-txns",
                          "non-leading-waiter", "keys-only",
                          "consequence-non-subset", "two-txns"},
        "RankOK": {"two-lockers", "hold-vs-consequence", "non-leading-waiter",
                   "consequence-non-subset", "two-txns"},
        "NoCycle": {"two-lockers", "hold-vs-consequence", "non-leading-waiter",
                    "consequence-non-subset", "two-txns"},
        "NoStuck": {"two-lockers", "hold-vs-consequence", "non-leading-waiter",
                    "consequence-non-subset", "two-txns"},
    },
    "ctl-defer-submit": {
        "Inv_Isolation": {"consequence-non-subset", "deep-chain", "two-txns"},
    },
    "ctl-unseq-incr-txn": {
        "Inv_OneProspectiveLocker": {"two-lockers", "non-leading-waiter", "two-txns"},
    },
    "ctl-fifo-adopt": {
        "Inv_Isolation": {"consequence-non-subset"},
    },
    # A3.  Two HOLD_QUEUE holders on one entry need BOTH the pin and the guard gone: while
    # the holder is a fifo claim (O7) addFifo keeps it at the head, so no other task can
    # lead the entry and reach lockExclusive at all - which is why this profile also turns
    # off PUpgradeOnStart and therefore repeats ctl-no-upgrade's failures.  The row that
    # matters is Inv_AtMostOneLock, and it needs two INDEPENDENT tasks sharing a txnId: a
    # consequence inherits its submitter's stamp and sorts after it, so hold-vs-consequence
    # cannot build it (same reason two-lockers exists for ctl-unseq-incr-txn).
    "ctl-double-lock": {
        "Inv_AtMostOneLock": {"two-lockers", "non-leading-waiter",
                              "consequence-non-subset", "two-txns"},
        "Inv_LockerIsFifo": {"two-lockers", "hold-vs-consequence", "disjoint-txns",
                             "non-leading-waiter", "keys-only",
                             "consequence-non-subset", "two-txns"},
        "Inv_LockLeads": {"two-lockers", "hold-vs-consequence", "disjoint-txns",
                          "non-leading-waiter", "keys-only",
                          "consequence-non-subset", "two-txns"},
        "RankOK": {"two-lockers", "hold-vs-consequence", "non-leading-waiter",
                   "consequence-non-subset", "two-txns"},
        "NoCycle": {"two-lockers", "hold-vs-consequence", "non-leading-waiter",
                    "consequence-non-subset", "two-txns"},
        "NoStuck": {"two-lockers", "hold-vs-consequence", "non-leading-waiter",
                    "consequence-non-subset", "two-txns"},
    },
    "probe-bag-interleaves": {
        "Inv_Isolation": {"consequence-non-subset", "deep-chain", "two-txns"},
        "RankOK": {"keys-only", "non-leading-waiter", "consequence-non-subset",
                   "two-txns"},
        "NoCycle": {"non-leading-waiter", "consequence-non-subset"},
    },
}


def expected_failures(prof_name, topo_name):
    exp = EXPECT_FAIL.get(prof_name, {})
    return {inv for inv, topos in exp.items() if topo_name in topos}

def tla_set(xs):
    return "{" + ", ".join('"%s"' % x for x in xs) + "}"


def tla_seq_of_sets(xss):
    return "<<" + ", ".join(tla_set(xs) for xs in xss) + ">>"


def gen(dirname, name, topo, policy, liveness=False):
    n = len(topo["txns"])
    parent = topo.get("parent", [0] * n)
    with open(os.path.join(dirname, name + ".tla"), "w") as f:
        f.write("---- MODULE %s ----\n" % name)
        f.write("EXTENDS AccordExec\n")
        f.write("MCTaskTxns == %s\n" % tla_seq_of_sets(topo["txns"]))
        f.write("MCTaskKeys == %s\n" % tla_seq_of_sets(topo["keys"]))
        f.write("MCTaskParent == <<%s>>\n" % ", ".join(str(x) for x in parent))
        f.write("====\n")
    with open(os.path.join(dirname, name + ".cfg"), "w") as f:
        f.write("SPECIFICATION Spec\n")
        f.write("CHECK_DEADLOCK FALSE\n")
        f.write("CONSTANTS\n")
        f.write("  CmdEntries = %s\n" % tla_set(topo["cmd"]))
        f.write("  KeyEntries = %s\n" % tla_set(topo["key"]))
        f.write("  NumTasks = %d\n" % n)
        f.write("  TaskTxns <- MCTaskTxns\n")
        f.write("  TaskKeys <- MCTaskKeys\n")
        f.write("  TaskParent <- MCTaskParent\n")
        for k, v in policy.items():
            f.write("  %s = %s\n" % (k, str(v).upper() if isinstance(v, bool) else v))
        for inv in INVARIANTS:
            f.write("INVARIANT %s\n" % inv)
        if liveness:
            for p in PROPERTIES:
                f.write("PROPERTY %s\n" % p)


VIOLATION = re.compile(r"Invariant (\w+) is violated")


def completed_cleanly(out):
    """A run that did not complete proves nothing: a missing constant, a parse error and a
    timeout are all indistinguishable from a clean run if you test only for violations."""
    if "Model checking completed" not in out:
        bad = [l for l in out.splitlines() if l.startswith("Error:")]
        return bad[0][:90] if bad else "DID NOT COMPLETE"
    if "Error:" in out:
        bad = [l for l in out.splitlines() if l.startswith("Error:")]
        return bad[0][:90]
    return None


def write_cfg_checks(path, lines):
    with open(path) as f:
        cfg = [l for l in f if not l.startswith(("INVARIANT", "PROPERTY"))]
    with open(path, "w") as f:
        f.writelines(cfg)
        f.writelines(lines)


def run_one(topo_name, prof_name, timeout, no_probes=False, liveness=False):
    topo, policy = TOPOLOGIES[topo_name], PROFILES[prof_name]
    d = tempfile.mkdtemp(prefix="accordexec-")
    try:
        shutil.copy(SPEC, d)
        mod = "M"
        gen(d, mod, topo, policy, liveness)
        cfg_path = os.path.join(d, mod + ".cfg")
        cmd = ["tlc", "-workers", "2", "-cleanup", "-config", mod + ".cfg", mod + ".tla"]
        # TLC halts at the first violated invariant, so re-run without the ones
        # already known to fail until nothing new breaks
        failed, states, depth = [], 0, 0
        for _ in range(len(INVARIANTS) + 1):
            remaining = [i for i in INVARIANTS if i not in failed]
            if not remaining:
                break
            write_cfg_checks(cfg_path,
                             ["INVARIANT %s\n" % i for i in remaining])
            try:
                r = subprocess.run(cmd, cwd=d, capture_output=True, text=True,
                                   timeout=timeout)
            except subprocess.TimeoutExpired:
                return dict(topo=topo_name, profile=prof_name, failed=failed,
                            error="TIMEOUT", states=states, depth=depth, probes={})
            out = r.stdout
            m = re.search(r"(\d+) states generated, (\d+) distinct", out)
            if m:
                states = max(states, int(m.group(2)))
            m = re.search(r"depth of the complete state graph search is (\d+)", out)
            if m:
                depth = max(depth, int(m.group(1)))
            v = VIOLATION.search(out)
            if v:
                failed.append(v.group(1))
                continue
            bad = completed_cleanly(out)
            if bad:
                return dict(topo=topo_name, profile=prof_name, failed=failed,
                            error=bad, states=states, depth=depth, probes={})
            break
        # temporal properties, one at a time, and only once the invariant loop has
        # finished: TLC checks liveness periodically DURING the search and stops at the
        # first violation, so a property failure in the loop above would leave the safety
        # search incomplete while the remaining invariants still printed as `ok`.
        # notify.py does it this way too.
        for p in (PROPERTIES if liveness else []):
            write_cfg_checks(cfg_path, ["PROPERTY %s\n" % p])
            try:
                r = subprocess.run(cmd, cwd=d, capture_output=True, text=True,
                                   timeout=timeout)
            except subprocess.TimeoutExpired:
                return dict(topo=topo_name, profile=prof_name, failed=failed,
                            error="TIMEOUT", states=states, depth=depth, probes={})
            if "Temporal properties were violated" in r.stdout:
                failed.append(p)
                continue
            bad = completed_cleanly(r.stdout)
            if bad:
                return dict(topo=topo_name, profile=prof_name, failed=failed,
                            error=bad, states=states, depth=depth, probes={})
        # coverage probes: run each alone, "violated" == the situation is reached.  A probe
        # run that timed out or errored is UNKNOWN, not unreached - conflating the two would
        # silently downgrade a broken cell to a merely unexercised one.
        probes = {}
        for pr in ([] if no_probes else PROBES):
            write_cfg_checks(cfg_path, ["INVARIANT %s\n" % pr])
            try:
                r = subprocess.run(cmd, cwd=d, capture_output=True, text=True,
                                   timeout=timeout)
            except subprocess.TimeoutExpired:
                probes[pr] = "unknown"
                continue
            if "is violated" in r.stdout:
                probes[pr] = "reached"
            elif completed_cleanly(r.stdout) is None:
                probes[pr] = "unreached"
            else:
                probes[pr] = "unknown"
        return dict(topo=topo_name, profile=prof_name, failed=failed,
                    error=None, states=states, depth=depth, probes=probes)
    finally:
        shutil.rmtree(d, ignore_errors=True)


PSHORT = {
    "Probe_Contention": "cont", "Probe_AnyWait": "wait", "Probe_LockHeld": "lock",
    "Probe_LockHasWaiter": "lockQ", "Probe_LockHasFifoWaiter": "lockF",
    "Probe_BetweenRounds": "round",
    "Probe_ThresholdWait": "thresh", "Probe_BlockedOnTxnHoldingKeys": "keptK",
    "Probe_Consequence": "conseq", "Probe_TopLevelAtomic": "tlAtom",
    "Probe_UnitRevisits": "revisit",
}

SHORT = {
    "TypeOK": "Typ", "Inv_LockerIsFifo": "O7",
    "Inv_LockLeads": "O8", "Inv_OneProspectiveLocker": "1lk",
    "Inv_AtMostOneLock": "1lh",
    "Inv_Isolation": "Iso", "RankOK": "Rnk", "NoCycle": "Acy", "NoStuck": "Liv",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles", nargs="*", default=list(PROFILES))
    ap.add_argument("--topologies", nargs="*", default=list(TOPOLOGIES))
    ap.add_argument("--timeout", type=int, default=2400)
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--liveness", action="store_true",
                    help="also check Termination (temporal, needs the fairness conjunct)")
    ap.add_argument("--no-probes", action="store_true",
                    help="skip coverage probes (faster, but a green table may be vacuous)")
    a = ap.parse_args()

    if a.list:
        print("topologies:", " ".join(TOPOLOGIES))
        print("profiles:  ", " ".join(PROFILES))
        return 0

    jobs = [(t, p) for p in a.profiles for t in a.topologies]
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.jobs) as ex:
        futs = {ex.submit(run_one, t, p, a.timeout, a.no_probes, a.liveness): (t, p)
                for t, p in jobs}
        for fut in concurrent.futures.as_completed(futs):
            k = futs[fut]
            results[k] = fut.result()
            print(".", end="", flush=True)
    print()

    checks = INVARIANTS + (PROPERTIES if a.liveness else [])
    hdr = "%-33s %-19s %s | coverage reached" % ("profile", "topology", " ".join(
        "%-4s" % SHORT.get(i, i[:4]) for i in checks))
    print(hdr)
    print("-" * len(hdr))
    deviations = []
    worst = 0
    for p in a.profiles:
        broke_something = False
        for t in a.topologies:
            r = results[(t, p)]
            unchecked = r["error"] is not None
            cells = []
            for i in checks:
                cells.append("%-4s" % ("FAIL" if i in r["failed"]
                                       else ("?" if unchecked else "ok")))
            probes = r.get("probes", {})
            unk = [PSHORT[x] for x in PROBES if probes.get(x) == "unknown"]
            if a.no_probes:
                cov = "-"
            else:
                cov = " ".join(PSHORT[x] for x in PROBES
                               if probes.get(x) == "reached")
                miss = [PSHORT[x] for x in PROBES if probes.get(x) == "unreached"]
                if miss:
                    cov += "   (unreached: %s)" % ",".join(miss)
                if unk:
                    cov += "   (UNKNOWN: %s)" % ",".join(unk)
            note = ""
            if r["error"]:
                note = "  <<%s>>" % r["error"]
                worst = 2
                deviations.append("%s/%s did not complete: %s" % (p, t, r["error"]))
            else:
                # the contract: measured failures must equal the recorded expectation
                got, want = set(r["failed"]), expected_failures(p, t)
                if got - want:
                    deviations.append("%s/%s broke unexpectedly: %s"
                                      % (p, t, ", ".join(sorted(got - want))))
                if want - got:
                    deviations.append(
                        "%s/%s no longer breaks %s - the control may have become vacuous"
                        % (p, t, ", ".join(sorted(want - got))))
                if got:
                    broke_something = True
            if unk:
                deviations.append("%s/%s probe run(s) inconclusive: %s"
                                  % (p, t, ",".join(unk)))
            print("%-33s %-19s %s | %s%s" % (p, t, " ".join(cells), cov, note))
        if p in CONTROLS and not broke_something:
            witnesses = set().union(*EXPECT_FAIL[p].values()) if EXPECT_FAIL.get(p) else set()
            if witnesses & set(a.topologies):
                deviations.append(
                    "%s broke NOTHING on any topology run - a control that breaks nothing "
                    "is no longer evidence for anything" % p)
            else:
                # a control cannot fail on a topology that cannot build the situation it
                # enables, so a partial run that excludes every witness is not a deviation
                print("    (note: this run includes none of %s's witness topologies, "
                      "so the control is not exercised)" % p)
        print()

    if deviations:
        worst = max(worst, 1)
        print("DEVIATIONS FROM THE EXPECTED MATRIX (%d):" % len(deviations))
        for d in deviations:
            print("  ! " + d)
    else:
        print("matrix matches EXPECT_FAIL; every control run broke its property.")
    return worst


if __name__ == "__main__":
    sys.exit(main())
