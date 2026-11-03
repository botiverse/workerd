# Cluster-mode alarm cases (Botiverse)

Alarm semantics with several cluster-mode workerd processes on one machine. Each case starts its own
fresh cluster of 3 processes (ports 8900-8902, a new shared directory under `/tmp`), and the script
exits 1 if any case fails.

```sh
bazel build //src/workerd/server:workerd --config=release
node botiverse/alarm-test/cases.mjs            # all cases
node botiverse/alarm-test/cases.mjs "leader"   # cases whose name contains "leader"
WORKERD=/path/to/workerd node botiverse/alarm-test/cases.mjs
```

The cases:

- the alarm time moved several times: it runs once, at the latest time;
- the handler throws twice: it is retried with backoff and succeeds on the third run;
- the handler sets its next alarm itself (`setAlarm()` inside `alarm()`), twice: it runs three times, and no alarm is left;
- `deleteAlarm()` before it is due: it never runs;
- the process holding the object is killed before the alarm is due: the leader claims the object and runs it once;
- the holder is killed in the middle of the handler: the alarm is re-run elsewhere and recorded once;
- the alarm leader is killed while it waits for a running handler: the alarm runs once and a new leader takes over;
- the leader is killed with an alarm pending: another process takes over and runs it once;
- 300 alarms due at the same moment: each runs exactly once;
- all processes set 600 alarms at once: no `SQLITE_BUSY`, all run once.

Needs Node 22+. Linux only (cluster mode uses OFD locks). Upstream's own cluster test
(`src/workerd/server/tests/cluster`, `cluster-test-unix` / `cluster-test-cidr`) covers the
`network = "unix"` alarm path and the rejection across machines.
