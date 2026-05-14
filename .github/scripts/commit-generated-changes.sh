#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -lt 2 ]]; then
  echo "Usage: $0 <commit-message> <path> [path ...]" >&2
  exit 2
fi

commit_message="$1"
shift

branch="${GITHUB_REF#refs/heads/}"
if [[ -z "$branch" || "$branch" == "$GITHUB_REF" ]]; then
  branch="$(git rev-parse --abbrev-ref HEAD)"
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

git add "$@"
if git diff --cached --quiet; then
  echo "No changes to commit"
  exit 0
fi

# Scheduled workflows can sit in the concurrency queue while earlier runs push
# generated state. Rebase onto the latest branch head immediately before
# committing so the final push starts from the freshest known remote state.
stash_name="generated-before-commit-$(date +%s)"
git stash push --include-untracked -m "$stash_name"
git fetch --no-tags origin "$branch"
git reset --hard "origin/$branch"
if git stash list | grep -Fq "$stash_name"; then
  git stash pop
fi

git add "$@"
if git diff --cached --quiet; then
  echo "No changes to commit after refreshing branch head"
  exit 0
fi

git commit -m "$commit_message"

for attempt in 1 2 3; do
  if git push origin "HEAD:$branch"; then
    exit 0
  fi

  echo "Push rejected on attempt ${attempt}; rebasing onto origin/${branch} and retrying"
  git fetch --no-tags origin "$branch"
  if ! git rebase "origin/$branch"; then
    git rebase --abort || true
    echo "::error::Unable to rebase generated commit onto origin/${branch}. Manual inspection required."
    exit 1
  fi
done

git push origin "HEAD:$branch"
