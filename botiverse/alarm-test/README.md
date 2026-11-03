# Cluster-mode alarm tests (Botiverse)

Runs three cluster-mode workerd processes on one machine (ports 8900-8902, shared directory
`/tmp/alarm-test-data`) and checks alarm semantics:

1. the alarm time moved several times: it runs once, at the latest time;
2. the handler throws twice: it is retried with backoff and succeeds on the third run;
3. the process holding the object is killed: the alarm leader claims the object and runs it once;
4. the alarm leader is killed: another process takes over and still runs the pending alarm once.

```sh
bazel build //src/workerd/server:workerd --config=release
botiverse/alarm-test/cases.sh        # WORKERD=<path> to use another binary
```

Prints PASS/FAIL per case. Needs `curl` and `node`. Linux only (cluster mode uses OFD locks).
