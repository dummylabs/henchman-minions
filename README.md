# henchman-minions

Minion repository for Henchman.

Each minion lives in its own directory:

```text
<minion_id>/
  manifest.yaml
  pyproject.toml
  uv.lock
  main.py
```

## After cloning

Enable repository-local git hooks:

```bash
git config core.hooksPath .githooks
```

This enables `.githooks/pre-commit`, which enforces version bumps.

## Version bump rule

If a commit changes a minion's Python files (`*.py`) or `manifest.yaml`, the same commit must also bump that minion's version in:

```text
<minion_id>/pyproject.toml
```

The hook checks staged files before commit and blocks the commit if `[project].version` was not changed.

Example workflow:

```bash
# edit alive_ping/main.py
# bump version in alive_ping/pyproject.toml, e.g. 0.1.0 -> 0.1.1
uv lock --directory alive_ping
git add alive_ping/main.py alive_ping/pyproject.toml alive_ping/uv.lock
git commit -m "Update alive ping behavior"
```

Bypass only intentionally:

```bash
git commit --no-verify
```
