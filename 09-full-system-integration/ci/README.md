# Why the contract gate is not in `.github/workflows/`

`p09-contracts.yml` belongs at `.github/workflows/p09-contracts.yml`. It is
parked here because the token that pushed this repository holds `repo` but not
`workflow` scope, and GitHub refuses any push whose tree creates or updates a
workflow file under those credentials:

    ! [remote rejected] master -> master (refusing to allow an OAuth App to
      create or update workflow `.github/workflows/p09-contracts.yml`
      without `workflow` scope)

Nothing about the file changed -- only its path. To put it back:

    gh auth refresh -s workflow
    git mv 09-full-system-integration/ci/p09-contracts.yml \
           .github/workflows/p09-contracts.yml
    git rm 09-full-system-integration/ci/README.md
    git commit -m "Restore the contract gate to .github/workflows/"
    git push

Until that happens the gate does not run on push, so the checks it owns are
unenforced on GitHub: the contract audit, the schema-sync check that keeps the
four copies of `market_data.proto` byte-identical, the release preflight that
fails a tag-only image pin, and two mutation controls that prove the first two
can actually fail. `make -C 09-full-system-integration lock` and
`p09 sync-schema --check` run all of it locally in the meantime.
