# CLI layout

The stable commands remain compatible. Short aliases are additive.

## Daily workflow

```text
mj status (st)             protection dashboard
mj health (doctor)         scriptable health result
mj snapshot (snap)         capture roots
mj sync (push)             publish to remote
mj restore (recover)       plan/apply recovery
```

## Configuration/history

`init`, `root`, `branch`, `log`, `diff`, `op`, and `key`.

## Remote/service

`remote`, `clone`, `lifecycle`, `daemon` (`service`), and `watch`.

## Advanced maintenance

`state`, `fsck` (`check`), `pack`, `prune`, `gc`, `cache`, `event`, `large`,
`mount`, `unmount`, and `hydrate`.
