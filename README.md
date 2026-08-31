# embedded-tikv

[![Version](https://img.shields.io/github/tag/occamist/embedded-tikv.svg)](https://github.com/occamist/embedded-tikv/tags)
[![CI Build](https://github.com/occamist/embedded-tikv/actions/workflows/test.yaml/badge.svg)](https://github.com/occamist/embedded-tikv/actions/workflows/test.yaml)
[![GoDoc](https://godoc.org/github.com/occamist/embedded-tikv?status.svg)](https://godoc.org/github.com/occamist/embedded-tikv)
[![License](https://img.shields.io/github/license/occamist/embedded-tikv)](https://github.com/occamist/embedded-tikv/blob/main/LICENSE)

Run a real [TiKV](https://tikv.org) cluster from your Go tests.

It is inspired by [`embedded-postgres`](https://github.com/fergusstrange/embedded-postgres),
and is roughly equivalent to `tiup playground --mode tikv-slim --without-monitor`, minus
everything a test does not need.

| Platform | Supported | First-run download |
|---|---|---|
| linux/amd64, linux/arm64 | yes | ~487 MB |
| darwin/amd64, darwin/arm64 | yes | ~109 MB |
| windows | **no — does not compile** | — |

TiKV publishes no Windows binaries. [tikv/tikv#9103](https://github.com/tikv/tikv/issues/9103).
Linux `tikv-server` binary is roughly 9x bigger than macOS binary because it ships with
debug symbols. Downloads are also cached per version under `~/.embedded-tikv`.

`embedded-tikv` starts one `pd-server` and one or more `tikv-server` processes on ephemeral
ports, waits until the cluster is genuinely writable, and tears everything down afterwards.
Binaries are official builds, downloaded once and cached in your home directory. There is no
Docker, no `tiup`, and no cluster to manage.

```sh
go get github.com/occamist/embedded-tikv
```

A TiKV cluster per test gives complete isolation. A full start/stop cycle costs roughly 1.5
seconds, so this is affordable for a decent number of tests:

```go
cluster := embeddedtikv.New()
if err := cluster.Start(); err != nil {
    t.Fatal(err)
}
t.Cleanup(func() {
    if err := cluster.Stop(); err != nil {
        t.Error(err)
    }
})

client, err := rawkv.NewClientWithOpts(ctx, cluster.Endpoints())
// ...
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
    if err := cluster.Stop(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
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

## CI & Air-gapped Environments

Point the library at binaries you have already staged, and nothing is ever downloaded:

```
EMBEDDED_TIKV_BINARIES=/opt/tikv/bin go test ./...
```

The directory needs `tikv-server` and `pd-server`. `Config.BinariesPath` does the same thing in
code and takes precedence over the environment variable.

Otherwise, cache `~/.embedded-tikv` between CI runs.
