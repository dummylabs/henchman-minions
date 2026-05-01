#!/usr/bin/env bash
set -euo pipefail

# Commit and push all uncommitted changes in this repository.
# Usage: ./commit_and_push.sh

repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" || {
  echo "Error: not inside a git repository" >&2
  exit 1
}
cd "$repo_root"

if git diff --quiet --ignore-submodules -- && git diff --cached --quiet --ignore-submodules -- && [ -z "$(git ls-files --others --exclude-standard)" ]; then
  echo "No uncommitted changes found. Nothing to commit."
  exit 0
fi

echo "Uncommitted changes:"
git status --short

echo
read -r -p "Commit comment: " commit_message
if [ -z "${commit_message//[[:space:]]/}" ]; then
  echo "Error: commit comment must not be empty" >&2
  exit 1
fi

git add -A

if git diff --cached --quiet --ignore-submodules --; then
  echo "No staged changes after git add. Nothing to commit."
  exit 0
fi

git commit -m "$commit_message"

current_branch="$(git branch --show-current)"
if [ -z "$current_branch" ]; then
  echo "Error: detached HEAD; cannot determine branch to push" >&2
  exit 1
fi

if git rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1; then
  git push
else
  git push -u origin "$current_branch"
fi
