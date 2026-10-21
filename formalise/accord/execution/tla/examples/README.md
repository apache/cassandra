Single-run examples, for iterating without a driver.  Copy the module in first:

    cp ../AccordExec.tla   . && tlc -cleanup -config Baseline.cfg Baseline.tla
    cp ../AccordNotify.tla . && tlc -cleanup -config Notify.cfg   Notify.tla

Baseline  the hold-vs-consequence topology under production settings.
Notify    three tasks, one command entry, two key entries, mixed regions and
          sync-ness: enough for a task to be demoted on a txnId while holding key
          positions, which is the state the two waitingFor counters must survive.

Neither cfg lists the coverage probes, because each is a negated reachability claim and
TLC halts at the first one it reports violated - so a single run would only ever tell you
about the first.  Use the drivers for those, which run them one at a time and assert in
both directions (`../matrix.py` for AccordExec, `../notify.py` for AccordNotify).
