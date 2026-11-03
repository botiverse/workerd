#!/bin/sh
# Imprint's four alarm cases (#wg-deck:ee25001f 7b920810) on a 3-process cluster. Prints PASS/FAIL per case.
#   1. the alarm comes due while no process holds the object -> the leader claims and runs it
#   2. the process holding the object dies -> another runs it, once
#   3. the handler throws -> retried with backoff
#   4. the time is moved several times -> it runs once, at the latest time
# Plus: the leader process dies -> another becomes leader and still runs a pending alarm, once.
here=$(cd "$(dirname "$0")" && pwd); N="$here/nodes.sh"
get() { curl -sS -m 10 "http://127.0.0.1:$1/$2"; }
fired() { get $1 "state?do=$2" | node -e 'let s="";process.stdin.on("data",d=>s+=d).on("end",()=>{const j=JSON.parse(s);console.log(j.fired.length, JSON.stringify(j.fired.map(f=>f.node+(f.isRetry?"(retry)":""))))})'; }
leader() { grep -l 'now the alarm leader' /tmp/alarm-test-*.log 2>/dev/null | sed 's/.*alarm-test-\([0-9]*\).log/\1/' | tail -1; }
check() { # name expectedCount port do
  r=$(fired $3 $4); n=${r%% *}
  if [ "$n" = "$2" ]; then echo "PASS $1: fired $r"; else echo "FAIL $1: fired $r (want $2)"; fi
}

$N stop; $N start 3; sleep 2
L=$(leader); echo "leader: n$L"
# pick two non-leader ports
others=""; for i in 0 1 2; do [ "$i" != "$L" ] && others="$others $i"; done
set -- $others; A=$1; B=$2

# case 4: move the time several times; must run once, at the latest time (~3 s)
get $((8900+A)) "set?do=c4&ms=10000" >/dev/null; get $((8900+B)) "set?do=c4&ms=1000" >/dev/null; get $((8900+A)) "set?do=c4&ms=3000" >/dev/null
sleep 1.6; r=$(fired $((8900+A)) c4); [ "${r%% *}" = "0" ] && echo "PASS 4a: not run at the superseded 1 s" || echo "FAIL 4a: ran early: $r"
sleep 3; check "4 (latest time only)" 1 $((8900+B)) c4

# case 3: throws twice, then succeeds (retries at ~2 s and ~4 s)
get $((8900+A)) "throw?do=c3&n=2" >/dev/null; get $((8900+A)) "set?do=c3&ms=500" >/dev/null
sleep 9; check "3 (retried after throwing twice)" 3 $((8900+A)) c3

# case 1 + 2: set on A, then kill A (the holder); the leader must claim the object and run it once
get $((8900+A)) "set?do=c12&ms=3000" >/dev/null; $N kill $A; sleep 10
check "1+2 (holder died, unheld object claimed)" 1 $((8900+L)) c12

# leader death: alarm pending, kill the leader; the remaining process takes over and runs it once
get $((8900+B)) "set?do=cL&ms=4000" >/dev/null; $N kill $L; sleep 7
check "leader failover" 1 $((8900+B)) cL
grep -h 'now the alarm leader' /tmp/alarm-test-*.log | wc -l | xargs echo "leader elections:"
$N stop
