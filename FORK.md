# NebInfra Asynq fork

This repository is a first-party fork of
[`github.com/hibiken/asynq`](https://github.com/hibiken/asynq). The fork starts
from upstream commit `d704b68a426d1d3a6c707f9661d29296e1350775`, the commit
tagged `v0.26.0`.

The `receiver-v0.26.0` branch is the immutable base for receiver-transition
changes.

The fork changes the Go module path to `github.com/nebinfra/asynq`. Upstream
copyright and MIT license terms remain in [`LICENSE`](LICENSE).

Receiver transition work is tracked by
[`nebinfra/nebinfra#3326`](https://github.com/nebinfra/nebinfra/issues/3326).
Unmarked task behavior remains compatible with the pinned upstream base.
