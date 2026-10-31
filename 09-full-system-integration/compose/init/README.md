# Generated init jobs

`p09 render-init` wrote everything here from
`../../contracts/contracts.lock.yaml`. Do not edit these files: the next render
overwrites them, and a hand-edited init job is the drift they exist to prevent.

Run them where the Docker daemon is. On this machine that is inside WSL, so the
scripts reach the services through `docker exec` rather than over a socket --
WSL's localhost forwarding does not bridge 9092 or 8123 to Windows, and a script
that assumed it did would fail in a way that looks like a broken broker.

```sh
cd 09-full-system-integration/compose
docker compose -f compose.dev.yaml up -d      # wait for both healthchecks
sh init/01-topics.sh                          # create, then describe and decide
sh init/02-apply-ddl.sh                       # apply P05 + P06 DDL by path
sh init/03-verify.sh > /tmp/p09-infra.json    # observe only
```

Then, wherever `p09` is installed:

```sh
p09 verify-infra --observed /tmp/p09-infra.json
```

`03-verify.sh` reports and `verify-infra` judges, deliberately: a script that
decided its own verdict would be marking its own work.

Exit codes. `01-topics.sh` returns 1 if a topic has the wrong partition count or
a broker default re-enables auto-creation. `02-apply-ddl.sh` returns non-zero on
a missing file or a failed statement. `03-verify.sh` returns 0 whenever it
managed to observe -- including when what it observed is wrong, because that is a
verdict and verdicts are not its job.
