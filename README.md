# embedded-tikv

Run a real [TiKV](https://tikv.org) cluster from your Go tests.

It is inspired by [`embedded-postgres`](https://github.com/fergusstrange/embedded-postgres),
and is roughly equivalent to `tiup playground --mode tikv-slim --without-monitor`, minus
everything a test does not need.

`embedded-tikv` starts one `pd-server` and one or more `tikv-server` processes on ephemeral
ports, waits until the cluster is genuinely writable, and tears everything down afterwards.
Binaries are official builds, downloaded once and cached in your home directory. There is no
Docker, no `tiup`, and no cluster to manage.

```sh
go get github.com/occamist/embedded-tikv
```

```go
func TestThing(t *testing.T) {
    cluster := embeddedtikv.New()
    if err := cluster.Start(); err != nil {
        t.Fatal(err)
    }
    t.Cleanup(func() { _ = cluster.Stop() })

    client, err := rawkv.NewClientWithOpts(ctx, cluster.Endpoints())
    // ...
}
```

## Choosing a usage pattern

A TiKV cluster per test gives complete isolation. A full start/stop cycle costs roughly 1.5
seconds, so this is affordable for a decent number of tests:

```go
cluster := embeddedtikv.New()
if err := cluster.Start(); err != nil {
    t.Fatal(err)
}
t.Cleanup(func() { _ = cluster.Stop() })
```

A TiKV cluster per package, started in `TestMain` and shared, keeps a large suite fast.

```go
var cluster *embeddedtikv.Cluster

func TestMain(m *testing.M) {
    cluster = embeddedtikv.New()
    if err := cluster.Start(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    code := m.Run()
    _ = cluster.Stop()
    os.Exit(code)
}
```

Both patterns are demonstrated in [`examples/`](examples).

## Configuration

`New()` uses `DefaultConfig()`. Every setting is an immutable builder method:

```go
cluster := embeddedtikv.New(embeddedtikv.DefaultConfig().
    Version(embeddedtikv.V8_5).
    TiKVCount(3).
    StartTimeout(90 * time.Second).
    Logger(os.Stdout))
```

| Method | Default | Notes |
|---|---|---|
| `Version` | `V8_5` (`v8.5.8`) | Also `V8_1`, `V7_5`, `V7_1`, `V6_5`, or any tag on the mirror |
| `TiKVCount` | `1` | There is always exactly one PD |
| `Host` | `127.0.0.1` | |
| `DataPath` | temporary directory | When set, it survives `Stop` — but see below |
| `BinariesPath` | unset | Skips downloading entirely |
| `CachePath` | `~/.embedded-tikv` | Shared by every project on the machine |
| `MirrorURL` | `https://tiup-mirrors.pingcap.com` | For an internal mirror |
| `StartTimeout` | 120s | Excludes download time |
| `Logger` | unset | Receives a copy of all server output |
| `ReapOrphans` | `true` | Sweep clusters abandoned by earlier runs on `Start` |
| `PDConfig` / `TiKVConfig` | see below | Dotted TOML keys merged over the defaults |

### Defaults that are not TiKV's defaults

TiKV and PD ship defaults tuned for a dedicated production node. These overrides are what make a
cluster usable inside a test, and each is applied for a specific reason:

| Setting | Value | Why |
|---|---|---|
| `replication.max-replicas` | `1` when `TiKVCount < 3` | Fewer than three stores can never reach quorum on three replicas, so regions would stay unavailable forever |
| `schedule.low-space-ratio` | `1.0` | A nearly-full CI disk otherwise stops PD scheduling to the only store |
| `schedule.patrol-region-interval` | `100ms` | Lets a fresh cluster settle in seconds |
| `storage.reserve-space` | `0` | TiKV reserves 5 GiB by default and refuses to start without it |
| `storage.block-cache.capacity` | `128MB` | The default is **45% of system RAM**, untenable with several clusters at once |
| `rocksdb`/`raftdb.max-open-files` | `256` | Keeps clear of low file-descriptor limits |
| `election-interval` / `tick-interval` (PD) | `500ms` / `50ms` | A single-member PD still idles through a full election timeout before campaigning — a measured median of 1.65s, and up to 3.15s. A lone member cannot lose an election |
| `raftstore.pd-store-heartbeat-tick-interval` | `500ms` | PD marks a newly registered store `Down` until its first store heartbeat; at the stock 10s this alone adds ~10s to `Start`. Governs liveness reporting only, not the data path |
| `server.graceful-shutdown-timeout` | `0s` | Bounds **one phase** of shutdown: handing region leaders to a surviving peer. `Stop` tears down the whole cluster, so there is never a surviving peer — TiKV just waits out the timeout (measured 20.6s per `Stop`) and exits anyway |

`raftstore.pd-heartbeat-tick-interval` — the *region* heartbeat, which drives PD's region view
and scheduling — is deliberately **left at TiKV's default**. It does not affect how quickly a
cluster becomes usable, so overriding it would change behaviour under test for no gain.

Setting `graceful-shutdown-timeout` to `0s` does not disable graceful shutdown. The rest of the
sequence still runs in full: raftstore stop, batch-system drain, RocksDB flush, `Storage stopped`.

Override any of them, or add your own:

```go
embeddedtikv.DefaultConfig().TiKVConfig(map[string]any{
    "storage.api-version": 2,
    "storage.enable-ttl":  true,
})
```

## Platform support

| Platform | Supported | First-run download |
|---|---|---|
| linux/amd64, linux/arm64 | yes | ~487 MB |
| darwin/amd64, darwin/arm64 | yes | ~109 MB |
| windows | **no — does not compile** | — |

The Linux `tikv-server` is roughly nine times larger than the macOS one because it ships with
debug symbols. Downloads are cached per version under `~/.embedded-tikv` and shared across every project on the machine, so this cost is paid once.

TiKV publishes no Windows binaries, so there is nothing for this library to run. Upstream has
never shipped them: [tikv/tikv#9103](https://github.com/tikv/tikv/issues/9103) requested an
official Windows build in 2020 and is still open.

**Every official build is dynamically linked, and there is no static variant.** The TiUP mirror
publishes only `linux/{amd64,arm64}` and `darwin/{amd64,arm64}` for `tikv` and `pd`.

## Abandoned clusters

If a test binary dies without reaching `Stop` — `SIGKILL`, a panic in `TestMain`, Ctrl-C — the
cluster tears itself down anyway.

Each server is started under a **parent-death watchdog**. The library holds the write end of a
pipe whose read end the server's supervising shell watches, and that read blocks until one of two
things happens.

If this process exits, the kernel closes the write end and the read reports EOF — however the
process dies, `SIGKILL` included. The watchdog then kills the server, removes the temporary data
directory, and finally clears the registry record — in that order, so that while the record
survives a later sweep can still find anything the watchdog failed to clean up. A parent killed
outright therefore leaves nothing at all behind.

If the server is instead being stopped deliberately, the library writes a line to the pipe first
and the watchdog stands down without touching anything. That has to be explicit: stopping a
server that has already crashed sends it no signal, so a watchdog inferring intent from silence
would delete the whole cluster's data directory while its siblings were still running.

The shell `exec`s the server, so the server keeps its own PID and process group — signals, exit
status and log capture behave exactly as a direct spawn.

This is the mechanism on **every** platform, including Linux. Linux's `Pdeathsig` is not used
alongside it, for two reasons. The kernel delivers that signal when the *creating thread* exits,
which the Go runtime may do while the process is perfectly healthy
([go.dev/issue/27505](https://go.dev/issue/27505), closed in 2026 by documenting the hazard
rather than removing it) — a server killed at random mid-test. And relying on it would mean
Linux ran a different mechanism from macOS, leaving the portable watchdog unexercised on the
machines most tests run on. `Pdeathsig` is not requested anywhere.

A directory you supplied via `DataPath` is yours and is never removed by the watchdog. If
`/bin/sh` is absent, as in some distroless images, servers are started directly and there is no
parent-death mechanism at all — the sweep below is then the only thing that reclaims them, on
the next `Start` or an explicit `CleanOrphans`.

The sweep is the backstop for anything the watchdog could not handle — a `SIGKILL`ed watchdog, a
machine that lost power, a host with no `/bin/sh`. `Start` and `Stop` both sweep automatically —
`Stop` reclaims anything abandoned by another process while this cluster was running — and you
can sweep explicitly from a `TestMain` or CI pre-step:

```go
reclaimed, err := embeddedtikv.CleanOrphans()
```

It runs three passes, because a record alone cannot describe every way a cluster can be
abandoned:

1. **Records** — the ordinary case. Kills the servers a record names, then removes its data
   directory.
2. **Data directories with no usable record** — a cluster killed part-way through startup may
   have a directory but no record naming it. Directories are named `embedded-tikv-<id>` after
   the record that owns them, so the two can always be paired up again.
3. **Locks with no record** — left by a crash between claiming an identity and writing it. Tiny,
   but they would accumulate forever.

Pass 1 also kills any process whose command line references the cluster's data directory, not
just the PIDs the record lists. That covers a server started in the instant before its PID was
recorded.

The sweep is conservative by construction and cannot disturb a running cluster:

- A live cluster holds an advisory lock on its record for its whole lifetime. Only the kernel
  releases it, and only when that process exits — however it exits. A record whose lock can be
  taken therefore has a definitively dead owner. This is immune to PID reuse, which a recorded
  PID alone is not.
- The lock is taken *before* either the record or the data directory exists, and a sweep never
  creates a missing lock file. Together these mean a sweep cannot observe a cluster that is
  still being set up and mistake it for an abandoned one.
- A data directory with no lock at all — because the registry was wiped, or belongs to a cluster
  configured with a different `CachePath` and so invisible here — is only reclaimed once it has
  gone untouched for five minutes *and* no live process references it.
- Before anything is killed, the recorded PID must still be running a process whose command line
  names **both** the expected binary **and** that cluster's own data directory. Because the data
  directory is unique per cluster, an unrelated TiKV — including a production one started from
  the same binary — can never match.

Set `ReapOrphans(false)` to opt out of the automatic sweep.

### DataPath is not reused across runs

Setting `DataPath` keeps the directory and its logs after `Stop`, which is what you want when
inspecting a failure. It does **not** carry cluster state from one run to the next: every
`Start` clears the `pd/` and `tikv-N/` subdirectories first. PD records its own peer URL in its
data directory, and this library allocates fresh ports on every start, so a reused directory
would refer to a cluster that no longer exists.

Treat `DataPath` as somewhere to look after a run, not as storage.

## Ctrl-C and other signals

Neither `defer` nor `t.Cleanup` runs when a process is signalled. Measured on Linux:

| termination | `defer` / `t.Cleanup` |
|---|---|
| normal return, `t.Fatal`, panic | runs |
| Ctrl-C (`SIGINT`) | **skipped** |
| `SIGTERM` (CI cancel, `docker stop`) | **skipped** |
| `SIGKILL` | **skipped** |
| `go test -timeout` expiry | **skipped** |
| `os.Exit` in `TestMain` | **skipped** |

The watchdog covers all of these — nothing is left behind either way, though it does so by
`SIGKILL`. Most suites therefore need nothing here, and `SIGKILL` and `go test -timeout` cannot
be caught by anyone in any case.

If you want a *graceful* shutdown on Ctrl-C instead — worth it when you are about to read
TiKV's logs, or care about RocksDB closing tidily — handle the signal yourself. `Stop` is safe
to call from another goroutine:

```go
sigs := make(chan os.Signal, 1)
signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

go func() {
    s := <-sigs
    _ = cluster.Stop()
    signal.Reset(s)
    _ = syscall.Kill(os.Getpid(), s.(syscall.Signal)) // die as we would have
}()
```

The re-raise is the part worth keeping. Without it the signal is swallowed: the cluster stops
but the run carries on, no longer interruptible by the usual means.

`signal.NotifyContext` is tidier if you are already threading a context, but it does not report
*which* signal fired and leaves its handler installed — so the process will not die on Ctrl-C
until you call its stop function and re-raise anyway.

This is deliberately yours rather than the library's. Signal handling is process-global and
would fight a handler of your own, and the exit policy is the application's to choose.

## CI and air-gapped environments

Point the library at binaries you have already staged, and nothing is ever downloaded:

```
EMBEDDED_TIKV_BINARIES=/opt/tikv/bin go test ./...
```

The directory needs `tikv-server` and `pd-server`. `Config.BinariesPath` does the same thing in
code and takes precedence over the environment variable.

Otherwise, cache `~/.embedded-tikv` between CI runs.
