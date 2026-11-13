# Container SQLite failures on arm64 macOS: emulation vs host bind mount

Task #384, a bounded experiment that follows #382 (note 728). It answers one question:
are the Bus errors and lost writes that `docker/replace-abrupt.sh` hit on the arm64
macOS daemon host caused by **amd64 emulation**, by **the macOS host bind mount**, or
only by **the two together**?

**Short answer: inconclusive. The results lean toward the bind mount.** Under the abrupt
harness, failures happened on the host bind mount both with emulation (amd64) and
without it (native arm64). So emulation is not needed for a failure. The minimal
sequential repro never failed in any cell. The abrupt harness was not run on a named
volume, so this experiment cannot show whether the bind mount is required.

## Environment (same for every batch)

| Item | Value | Source |
|---|---|---|
| Host | arm64 macOS (Darwin 25.2.0), 16 GiB RAM (`hw.memsize` 17179869184) | `sysctl` |
| Docker Desktop | 4.54.0, engine 29.1.2, server arch arm64, kernel 6.12.54-linuxkit | `Info.plist`, `docker version`, `docker info` |
| Docker VM | 4 CPUs, 8092 MiB memory, 1024 MiB swap, Apple Virtualization framework | Docker settings store (`Cpus`, `MemoryMiB`, `SwapMiB`, `UseVirtualizationFramework`) |
| File sharing backend | VirtioFS. Settings: `UseVirtualizationFrameworkVirtioFS=true` (`UseGrpcfuse=true` is also stored). The bind mount shows in the container as `/run/host_mark/private /data fakeowner …`, not `fuse.grpcfuse`. | settings store, container `/proc/mounts` |
| Named volume | `/dev/vda1 /data ext4` (VM disk, no host sharing) | container `/proc/mounts` |
| Emulation backend | Rosetta: binfmt `rosetta` entry enabled with interpreter `/run/rosetta/rosetta`, and an amd64 container process maps `/run/rosetta/rosetta`. QEMU binfmt entries exist too, but `x86_64` binaries ran through Rosetta. | privileged binfmt listing, `/proc/self/maps` |
| Host swap | 29.5–31.8 GiB used of 30.0–32.0 GiB across all runs, logged before each run (for example `total = 31744.00M used = 30204.94M` before the amd64 × bind abrupt run 3). This is a possible confounder. It was recorded, not controlled. | `sysctl vm.swapusage` per run log |

Images: `quorum-t384:amd64` and `quorum-t384:arm64`, built from the **unmodified**
repository `Dockerfile` with `docker build --platform linux/<arch>`. The arm64 build
works without changes. Its `codex-fetcher` stage always downloads the x86_64 Codex
binary, so Codex cannot run in the arm64 image. No step here runs Codex. Both images,
every container, and every volume were removed after the runs.

## Results

### Step 1–2: minimal sequential repro (N=300, 5 runs per cell)

| Image | DB storage | Runs | Pass | Fail | Failure kinds | Time per run |
|---|---|---|---|---|---|---|
| linux/amd64 (Rosetta) | host bind mount (VirtioFS) | 5 | 5 | 0 | — | 50–62 s |
| linux/amd64 (Rosetta) | named volume (ext4) | 5 | 5 | 0 | — | 46–60 s |
| linux/arm64 (native) | host bind mount (VirtioFS) | 5 | 5 | 0 | — | 21–26 s |
| linux/arm64 (native) | named volume (ext4) | 5 | 5 | 0 | — | 19–21 s |

Every run printed `calls_nonzero=0 notes=300 distinct=300 missing=0 max_id=300
integrity=ok`. That is 6,000 sequential committed writes with no lost rows and no
reused ids.

Passing run, amd64 × bind mount (run 1):

```
2026-11-12T20:42:46Z
vm.swapusage: total = 32768.00M  used = 31630.81M  free = 1137.19M  (encrypted)
uname=x86_64 mount=/run/host_mark/private /data fakeowner rw,nosuid,nodev,relatime,fakeowner 0 0
calls_nonzero=0 notes=300 distinct=300 missing=0 max_id=300 integrity=ok
RESULT=PASS
```

### Step 3: fallback, unmodified `docker/replace-abrupt.sh`, 5 runs per cell

The minimal repro found 0 failures in the amd64 × bind cell, so the fallback ran. It
used the amd64 × bind cell and one contrasting cell, arm64 × bind, which changes only
emulation.

| Image | DB storage | Runs | Pass | Fail | Failure kinds |
|---|---|---|---|---|---|
| linux/amd64 (Rosetta) | host bind mount (VirtioFS) | 5 | 3 | 2 | 2× `Bus error` from `quorum task-update` (runs 3 and 4). Both happened on the first seed write into replacement 4, right after it took authority. |
| linux/arm64 (native) | host bind mount (VirtioFS) | 5 | 4 | 1 | 1× crash image is `database disk image is malformed` (run 3). This is the post-SIGKILL DB + WAL copy taken after replacement 7. No Bus error. |
| either | named volume | not run | — | — | not run (outside the task's budget) |

The amd64 run used `docker/replace-abrupt.sh` exactly as checked in. That script checks
for `linux/amd64` / `x86_64`, so the arm64 cell used an out-of-tree copy with exactly two
lines changed: `REPLACE_PLATFORM=linux/arm64` and expected machine `aarch64`. Neither run
reproduced #382's other failure kind, where a `task-update` exited 0 and the next call
reused and replaced its row.

Failing run, amd64 × bind (run 3):

```
replace-abrupt: kill 0: wal=12392 shm=32768 bytes; integrity ok; 3 committed rows present
replace-abrupt: baseline recorded 25 artifacts; config sha256 d5331b2f8501b2c93ead6ad55f8eb1f51a6197c8f01c686b3aa93732ee28bacb
replace-abrupt: replacement 1: stale holder pid 17 -> serve pid 16; authority 34s after kill
replace-abrupt: kill 1: wal=712792 shm=32768 bytes; integrity ok; 6 committed rows present
replace-abrupt: replacement 2: stale holder pid 16 -> serve pid 15; authority 35s after kill
replace-abrupt: kill 2: wal=729272 shm=32768 bytes; integrity ok; 9 committed rows present
replace-abrupt: replacement 3: stale holder pid 15 -> serve pid 15; authority 35s after kill
replace-abrupt: kill 3: wal=712792 shm=32768 bytes; integrity ok; 12 committed rows present
replace-abrupt: replacement 4: stale holder pid 15 -> serve pid 14; authority 34s after kill
Bus error
replace-abrupt: seeding iteration 4 rows in quorum-replace-abrupt-r4-98836 failed
```

Run 4 in the same cell ended the same way: `replacement 4: … authority 34s after kill`,
then `Bus error`, then `seeding iteration 4 rows in quorum-replace-abrupt-r4-4243 failed`.

Passing run, amd64 × bind (run 1, final lines):

```
replace-abrupt: 12 abrupt replacements passed; serve pids 18 14 14 15 15 16 15 15 16 16 15 15 15; stale-holder PID reused 6 times; 12 kills left a non-empty WAL
replace-abrupt: contender serve pids 15 all exited 2 against holder serve pid 15 (equal on holder round 1 attempt 1); holder remains sole authority
replace-abrupt: final artifacts, config (sha256 d5331b2f8501b2c93ead6ad55f8eb1f51a6197c8f01c686b3aa93732ee28bacb), and 36 committed rows unchanged
replace-abrupt: abrupt replacement and equal-PID contention passed
```

Failing run, arm64 × bind (run 3):

```
replace-abrupt: kill 5: wal=642752 shm=32768 bytes; integrity ok; 18 committed rows present
replace-abrupt: replacement 6: stale holder pid 15 -> serve pid 14; authority 36s after kill
replace-abrupt: kill 6: wal=12392 shm=32768 bytes; integrity ok; 21 committed rows present
replace-abrupt: replacement 7: stale holder pid 14 -> serve pid 15; authority 39s after kill
Error: stepping, database disk image is malformed (11)
replace-abrupt: DB query failed
```

The error comes from `replace_snapshot` (`docker/replace-lib.sh`). It runs host
`sqlite3` against the DB + WAL that it copied off the bind mount after the SIGKILL. At
that point the killed container is gone and nothing writes to the tree.
`replace_assert_recorded` fails before `replace_assert_integrity`, so this run has no
`integrity_check` value for that image.

Passing run, arm64 × bind (run 1, final lines):

```
replace-abrupt: 12 abrupt replacements passed; serve pids 18 15 14 15 15 15 16 15 16 15 15 14 14; stale-holder PID reused 4 times; 12 kills left a non-empty WAL
replace-abrupt: contender serve pids 15 15 15 15 16 15 15 14 all exited 2 against holder serve pid 14 (equal on holder round 1 attempt 8); holder remains sole authority
replace-abrupt: final artifacts, config (sha256 d5331b2f8501b2c93ead6ad55f8eb1f51a6197c8f01c686b3aa93732ee28bacb), and 36 committed rows unchanged
replace-abrupt: abrupt replacement and equal-PID contention passed
```

## Verified

1. With one SQLite connection at a time (single sequential `docker exec`, no daemon, no
   concurrent writers, no kills), there were no failures in any of the four cells:
   20 runs, 6,000 writes. That workload does not reproduce #382 on this host, with or
   without emulation, on a bind mount or a named volume.
2. With the abrupt harness (live `serve` plus overlapping writers plus SIGKILL),
   **native arm64 on the host bind mount failed** (1/5, malformed crash image). Emulation
   is not needed to get a failure on the bind mount.
3. The `Bus error` kind appeared only under Rosetta emulation on the bind mount (2/5).
   It did not appear in the native arm64 bind-mount cell (0/5).
4. Emulation is Rosetta and file sharing is VirtioFS (Environment table). Host swap was
   nearly full throughout every batch.

## Hypothesis (not verified here)

- **H1 — VirtioFS bind mount (likely).** SQLite in WAL mode needs every connection to
  see the same `-shm` shared-memory file (mmap) and the same POSIX locks. Both the Bus
  error and the malformed crash image fit a share that is not fully coherent. SIGBUS is
  the usual signal when a process touches an mmap'd page past the file end it sees.
  Both kinds appeared only on the bind mount and only under a live daemon with concurrent
  writers, and native-Linux CI (no VirtioFS) passes the same suites. Not shown here:
  the abrupt harness was never run on a named volume.
- **H2 — emulation makes it worse.** Bus errors appeared only under Rosetta. With n=5 per
  cell, 2/5 vs 0/5 does not show a real difference (by chance alone, 0/5 is plausible
  even at a 40% failure rate).
- **H3 — Quorum's own SQLite usage.** No evidence for it here. The minimal repro exercises
  Quorum's normal open/write/close path and never lost a row. If H1 were false, the
  remaining suspect would be how Quorum uses WAL while several connections are open.
- **H4 — host memory pressure.** Swap was nearly full. Its effect is unknown.

## Recommendation: (iii) inconclusive

This experiment rules out "emulation only": native arm64 failed on the bind mount. It
cannot tell "bind mount" apart from "bind mount + emulation", and it cannot rule out
Quorum's SQLite usage when connections overlap. The minimal repro never has two open
connections, so it cannot exercise shared-WAL coherence.

**One more bounded step that would decide it:** run the abrupt harness 10 times in the
**linux/amd64 × named volume** cell. That cell keeps emulation, which showed the highest
failure rate, and removes VirtioFS. Use an out-of-tree copy of
`docker/replace-abrupt.sh`/`replace-lib.sh` whose `/data` is a named volume and whose
snapshots use `docker cp` instead of host paths.

- **0/10 failures** (chance of that at the bind-mount rate of 2/5: about 0.6%) → choose
  recommendation (i). Document that a host bind-mounted Quorum DB is unsupported on Docker
  Desktop for macOS, and that a named volume is required.
- **Any failure on the named volume** → the bind mount is not required. Choose
  recommendation (ii). Follow up with a narrow investigation of Quorum's WAL use under
  overlapping connections (open/close and checkpoint path in `quorum-core` SQLite open,
  plus the last-close WAL deletion that `start_write_load` targets), run on native Linux
  with the same harness.

This experiment files no follow-up tasks. Until the step above runs, treat a host
bind-mounted DB on Docker Desktop for macOS as **unverified**, not supported.

## Repro script (step 1)

Kept out of the repository's gates. Usage:
`repro.sh <image> <platform> <bind|volume> [N]`. It runs one long-lived container
(`sleep infinity`, no daemon) and makes N sequential `docker exec … quorum task-update
--note-stdin` calls with distinct bodies. It then copies the DB out with `docker cp`
and checks with host `sqlite3`.

```sh
#!/bin/sh
# Minimal sequential-write repro for task #384. Usage:
#   repro.sh <image> <platform> <bind|volume> [N]
# One long-lived container (no daemon), one task, N sequential
# `quorum task-update --note-stdin` calls, each its own `docker exec`, with
# distinct bodies. Asserts: every call exited 0, note count == N with every
# body present, PRAGMA integrity_check == ok (host sqlite3 on a docker-cp copy).
set -u
IMAGE=$1 PLATFORM=$2 STORAGE=$3 N=${4:-300}
TAG=t384-$$
WORK=$(mktemp -d) && WORK=$(cd "$WORK" && pwd -P)
NAME=$TAG
VOL=$TAG
cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1
  if [ "$STORAGE" = bind ]; then
    docker run --rm --platform "$PLATFORM" --user 0 -v "$WORK/data:/data" \
      --entrypoint sh "$IMAGE" -c 'find /data -mindepth 1 -delete' >/dev/null 2>&1
  else
    docker volume rm "$VOL" >/dev/null 2>&1
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT
if [ "$STORAGE" = bind ]; then
  mkdir "$WORK/data"; MOUNT="$WORK/data:/data"
else
  docker volume create "$VOL" >/dev/null; MOUNT="$VOL:/data"
fi
docker run -d --name "$NAME" --platform "$PLATFORM" --user 0 \
  -e QUORUM_REPO=acme/widget -e QUORUM_HOME=/data/quorum -v "$MOUNT" \
  --entrypoint sleep "$IMAGE" infinity >/dev/null || exit 3
docker exec "$NAME" sh -c 'mkdir -p /data/quorum && chown -R 10001:10001 /data'
printf 'uname=%s mount=%s\n' "$(docker exec "$NAME" uname -m)" \
  "$(docker exec "$NAME" sh -c 'grep " /data " /proc/mounts')"
EX="docker exec -i -u 10001 -w /data $NAME"
echo body | $EX quorum task-create --created-by t384 --title probe --body-stdin \
  >/dev/null || { echo "FAIL create"; exit 1; }
fails=0
i=1
while [ "$i" -le "$N" ]; do
  out=$(printf 't384 note %04d' "$i" | $EX quorum task-update --agent t384 \
    --task-id 1 --note-stdin 2>&1 >/dev/null)
  rc=$?
  if [ "$rc" -ne 0 ]; then
    fails=$((fails + 1)); printf 'call %d exit %d: %s\n' "$i" "$rc" "$out"
  fi
  i=$((i + 1))
done
docker cp "$NAME:/data/quorum/repos/acme__widget" "$WORK/copy" >/dev/null || exit 3
DB=$WORK/copy/quorum.db
integrity=$(sqlite3 "$DB" 'PRAGMA integrity_check')
count=$(sqlite3 "$DB" "SELECT count(*) FROM task_notes WHERE task_id=1 AND agent='t384'")
distinct=$(sqlite3 "$DB" "SELECT count(DISTINCT body) FROM task_notes WHERE task_id=1 AND agent='t384'")
missing=$(i=1; while [ "$i" -le "$N" ]; do printf 't384 note %04d\n' "$i"; i=$((i+1)); done \
  | sort >"$WORK/want"; sqlite3 "$DB" "SELECT body FROM task_notes WHERE task_id=1 AND agent='t384'" \
  | sort | comm -23 "$WORK/want" - | wc -l | tr -d ' ')
maxid=$(sqlite3 "$DB" "SELECT max(id) FROM task_notes WHERE task_id=1 AND agent='t384'")
printf 'calls_nonzero=%d notes=%s distinct=%s missing=%s max_id=%s integrity=%s\n' \
  "$fails" "$count" "$distinct" "$missing" "$maxid" "$integrity"
if [ "$fails" -eq 0 ] && [ "$count" = "$N" ] && [ "$missing" = 0 ] \
  && [ "$integrity" = ok ]; then echo RESULT=PASS; else echo RESULT=FAIL; exit 1; fi
```

Matrix driver: for each of the four cells, it ran
`sh repro.sh quorum-t384:<arch> linux/<arch> <bind|volume> 300` 5 times. Before each
run it logged `date -u` and `sysctl vm.swapusage`. Every experiment used throwaway
`mktemp` trees and `t384-*` volumes, never the live daemon DB under `~/.quorum`.
