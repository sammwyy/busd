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

## Troubleshooting

Use the daemon's structured diagnostics to identify a denied operation by peer
ID, authenticated UID/GID, action, and target. Do not enable payload logging to
debug policy: the policy selectors above and broker lifecycle metadata are
sufficient for access-control incidents. If a process is unexpectedly denied,
confirm its kernel credentials and resolved executable, security label, and
cgroup before widening the rule.
