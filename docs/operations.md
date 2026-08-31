# BUS/1 operations guide

This guide describes the secure defaults, policy format, limits, and diagnostics
for a native `busd` deployment. BUS/1 payloads are opaque and are never logged
by default.

## Policy format

`bus-policy` parses the line-oriented, versioned `busd-policy/1` format. The
file must start with `version = 1`; its `default` decision is used when no rule
matches. Rules are evaluated in file order, so place specific denies before
broader allows. Values may be double-quoted when they contain whitespace.

```text
version = 1
default = deny

[[rule]]
effect = allow
actions = connect,subscribe,publish,send-peer,send-namespace
uid = 1000

[[rule]]
effect = allow
actions = claim-namespace
uid = 0
target = bus://system.*

[[rule]]
effect = allow
actions = monitor,register-dbus-name
uid = 0
```

The supported actions are `connect`, `claim-namespace`, `subscribe`,
`publish`, `send-peer`, `send-namespace`, `send-client`, `broadcast`,
`acknowledge`, `monitor`, and `register-dbus-name`. An empty `actions` value
matches every action. `target` matches the operation target exactly, or as a
prefix when it ends in `*`.

Rules can additionally match authenticated `uid`, `gid`, `executable`,
`security-label`, and `cgroup`, plus the claimed `client-id` and textual
`header.NAME` values:

```text
[[rule]]
effect = allow
actions = publish
uid = 1000
executable = /usr/libexec/example-worker
security-label = unconfined
cgroup = /user.slice/user-1000.slice
client-id = example-worker
header.role = producer
target = example.events.*
```

`uid`, `gid`, executable path, security label, and cgroup are resolved by the
broker and can be used for authorization. Client IDs and headers remain
client-claimed data; use them only as additional selectors after an
authenticated constraint. Unknown keys, unknown actions, missing rule effects,
and unsupported format versions reject the configuration.

## Safe baseline

Production configurations should use `default = deny`. In particular, do not
allow `broadcast`, `monitor`, or `register-dbus-name` unless a rule explicitly
grants them. The built-in allow-all policy is intended only for tests and an
explicitly unsafe development mode.

Malformed frames and packets larger than the configured maximum are rejected
on the affected connection. A protocol error is returned when possible, then
the peer is disconnected; the broker keeps serving other sessions.

The daemon applies these per-peer defaults: a 1 MiB packet, 4 MiB of retained
reliable-delivery data, 1,024 retained reliable deliveries, 128 in-flight
requests, 256 subscriptions, and 64 namespace claims. Override them at launch:

```text
busd daemon --socket /run/busd/busd.sock --policy /etc/busd/policy.conf \
  --maximum-frame-size 262144 --maximum-queued-bytes 1048576 \
  --maximum-queued-messages 256 --maximum-in-flight-requests 64 \
  --maximum-subscriptions 128 --maximum-namespace-claims 32
```

The broker applies authorization before selecting recipients or accepting a
reliable delivery, so priority or capacity settings cannot grant an otherwise
denied operation. `--unsafe-allow-all` is available only for explicit local
development and cannot be combined with `--policy`.

Reliable-delivery retries use seven priority levels: the optional unsigned
message header `priority` accepts `0` through `7`, where `7` is dispatched
first when multiple retry jobs are due. Priority is not an authorization
selector and never overrides a denied action. Retained reliable deliveries are
bounded by the queue limits above; excess work is rejected for that sender.

## Monitoring and metrics

The broker exposes a policy-gated monitoring API for integrations embedded with
the daemon. A monitor rule is required. Monitor records include the transition,
peer ID, destination label, logical message ID, and payload length. They never
contain payload bytes, headers, client-claimed metadata, or authenticated
credentials. The bounded history retains the most recent 1,024 records.

The broker metrics snapshot reports connected peers, namespaces, subscriptions,
messages and bytes routed, timeouts, retries, and dropped best-effort signals.
The daemon emits connection records as structured log fields and does not log
opaque payloads.

Check that a running daemon accepts native connections with:

```text
busd health --socket /run/busd/busd.sock
```

## Service installation and restart

Install the example unit from [`docs/systemd/busd.service`](systemd/busd.service),
create the unprivileged `busd` group, and install `/etc/busd/policy.conf` with
root ownership and mode `0640`. `/run/busd` is created by `RuntimeDirectory`
with mode `0750`; the daemon owns the socket and the group may connect subject
to policy. Do not place the socket in a world-writable directory.

The unit removes only `/run/busd/busd.sock` during the serialized systemd
start transition, allowing a stopped daemon to restart after an unclean exit.
Clients see the old session close and should use `ReconnectingBus`, which
replays claims and subscriptions and treats the replacement peer ID as new.
An upgrade should use `systemctl restart busd`; the integration suite covers
this stop, socket replacement, reconnect, and state restoration sequence.

## File descriptors and large payloads

The low-level Unix transport supports `SCM_RIGHTS` with a maximum of 16
descriptors per packet. Sends borrow descriptors; receives return CLOEXEC-owned
descriptors, and packets over the configured count are rejected. The broker
does not currently accept or forward descriptor-bearing BUS frames, so the
public client does not advertise `fd-passing` as a routed application feature.

Memfd payloads are explicitly unavailable in BUS/1-preview. The reserved
`memfd-payload-v1` capability name must not be offered until a future version
defines negotiation, descriptor ownership, lifetime, and broker forwarding.

## Verification

The workspace test suite includes deterministic arbitrary-input coverage for the
frame parser and randomized broker-state transitions. Run the standard native
checks before deployment changes:

```text
cargo fmt --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

## Troubleshooting

Use the daemon's structured diagnostics to identify a denied operation by peer
ID, authenticated UID/GID, action, and target. Do not enable payload logging to
debug policy: the policy selectors above and broker lifecycle metadata are
sufficient for access-control incidents. If a process is unexpectedly denied,
confirm its kernel credentials and resolved executable, security label, and
cgroup before widening the rule.
