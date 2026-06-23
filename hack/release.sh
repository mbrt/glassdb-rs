#!/usr/bin/env bash
#
# Cut a new GlassDB release from a maintainer's machine. Dry-run by default;
# pass --no-dry-run to actually publish.
#
# The nine library crates are published together in lockstep (one shared
# version); the two glassdb-bench-* crates stay private. See release-plz.toml
# and docs/releases.md.
#
# Flow (with --no-dry-run):
#   1. sanity checks (clean tree, on main, tooling + credentials present)
#   2. `make test-all` gate
#   3. `release-plz update` bumps every published crate in lockstep
#      (or `--version X.Y.Z` to force an exact version)
#   4. review the diff and confirm
#   5. commit + push the version bump
#   6. `cargo publish --workspace` publishes the crates to crates.io
#   7. create and push the single `vX.Y.Z` git tag
#   8. `gh release create vX.Y.Z --generate-notes` opens the GitHub release with
#      GitHub's auto-generated notes (no CHANGELOG.md is committed)
#
# Note: release-plz is used only for the version bump; the publish/tag/release
# steps are done with cargo and gh (see release-plz.toml for why).
#
# Usage:
#   hack/release.sh [--no-dry-run] [--yes] [--version X.Y.Z]
#
# Flags:
#   --no-dry-run     actually publish; without it the script only previews the
#                    bump and runs `cargo publish --workspace --dry-run`
#   --dry-run        force a dry run (the default); accepted for explicitness
#   --yes, -y        skip the interactive confirmation prompt
#   --version X.Y.Z  set an exact version instead of the auto-computed bump
#
# Environment / tools:
#   CARGO_REGISTRY_TOKEN  crates.io token used by `cargo publish` (real releases);
#                         optional if you've already run `cargo login`. If absent
#                         and not logged in, `cargo publish` just fails.
#   gh                    GitHub CLI, authenticated (for the GitHub release)
#   release-plz           auto-installed via `cargo install` if missing

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

usage() {
	# Print the leading comment block (from the summary line to the first blank
	# line), stripped of the leading "# ".
	sed -n '/^# Cut a new GlassDB/,/^$/{/^$/!p;}' "$0" | sed 's/^# \{0,1\}//'
}

die() {
	echo "error: $*" >&2
	exit 1
}

# The published crates, one per line, read from release-plz.toml -- the single
# source of truth for the published package set (see its [[package]] entries).
published_pkgs() {
	grep -E '^name = ' release-plz.toml | sed -E 's/^name = "([^"]+)".*/\1/'
}

DRY_RUN=true
ASSUME_YES=false
FORCE_VERSION=""

while [ $# -gt 0 ]; do
	case "$1" in
	--no-dry-run) DRY_RUN=false ;;
	--dry-run) DRY_RUN=true ;;
	--yes | -y) ASSUME_YES=true ;;
	--version)
		shift
		[ $# -gt 0 ] || die "--version requires an argument (e.g. --version 0.2.0)"
		FORCE_VERSION="$1"
		;;
	--version=*) FORCE_VERSION="${1#*=}" ;;
	-h | --help)
		usage
		exit 0
		;;
	*) die "unknown argument '$1' (try --help)" ;;
	esac
	shift
done

# --- Preconditions ----------------------------------------------------------

branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$branch" = "main" ] || die "must be on 'main' (currently on '$branch')"

if ! git diff --quiet || ! git diff --cached --quiet; then
	die "working tree is dirty; commit or stash changes first"
fi

if ! command -v release-plz >/dev/null 2>&1; then
	echo "==> release-plz not found; installing (cargo install --locked release-plz)"
	cargo install --locked release-plz
fi

if [ "$DRY_RUN" = false ]; then
	git remote get-url origin >/dev/null 2>&1 ||
		die "no 'origin' git remote (needed to push the commit, tag, and release)"
	git fetch --quiet origin main || die "git fetch origin main failed"
	if [ -n "$(git rev-list HEAD..origin/main)" ]; then
		die "local 'main' is behind origin/main; run 'git pull --ff-only' first"
	fi
fi

# Restore the working tree to its pre-bump state. Used on abort and after a dry
# run; safe because we required a clean tree above.
revert_tree() {
	git checkout -- .
}

# --- Test gate --------------------------------------------------------------

if [ "$DRY_RUN" = true ]; then
	echo "==> dry run: skipping 'make test-all'"
else
	echo "==> running test suite (make test-all)"
	make test-all
fi

# --- Version bump -----------------------------------------------------------

echo "==> bumping versions"
if [ -n "$FORCE_VERSION" ]; then
	# Force an exact version on every published crate (lockstep).
	mapfile -t pkgs < <(published_pkgs)
	[ "${#pkgs[@]}" -gt 0 ] || die "no published packages found in release-plz.toml"
	specs=()
	for p in "${pkgs[@]}"; do
		specs+=("$p@$FORCE_VERSION")
	done
	release-plz set-version "${specs[@]}"
else
	release-plz update
fi

# The lockstep version lives in [workspace.package] of the root Cargo.toml.
new_version="$(grep -m1 -E '^version = "' Cargo.toml | sed -E 's/^version = "([^"]+)".*/\1/')"
[ -n "$new_version" ] || die "could not determine the new version from Cargo.toml"
tag="v$new_version"

echo "==> proposed release: $tag"
git --no-pager diff -- '**/Cargo.toml' Cargo.toml Cargo.lock || true

# --- Dry run stops here -----------------------------------------------------

if [ "$DRY_RUN" = true ]; then
	# Verification reuses cached builds keyed by package id (name + version),
	# not source contents. An earlier dry-run at this same version can leave a
	# stale compiled crate that `cargo publish` silently reuses, so the verify
	# may pass or fail against outdated code. Drop just the workspace crates'
	# artifacts (third-party deps stay cached) to force a recompile from source.
	echo "==> dry run: cleaning workspace crate artifacts before verify"
	clean_args=()
	while IFS= read -r p; do clean_args+=(--package "$p"); done < <(published_pkgs)
	[ "${#clean_args[@]}" -gt 0 ] || die "no published packages found in release-plz.toml"
	cargo clean "${clean_args[@]}"

	echo "==> dry run: cargo publish --workspace --dry-run"
	cargo publish --workspace --dry-run --allow-dirty
	echo "==> reverting working-tree changes from dry run"
	revert_tree
	echo "dry run complete (would release $tag)."
	exit 0
fi

# --- Confirm ----------------------------------------------------------------

if [ "$ASSUME_YES" = false ]; then
	printf 'Proceed with release %s? [y/N] ' "$tag"
	read -r reply
	case "$reply" in
	y | Y | yes | Yes) ;;
	*)
		echo "aborted; reverting version bump."
		revert_tree
		exit 1
		;;
	esac
fi

# --- Commit, publish, tag, release ------------------------------------------

# The first release (0.1.0) may be a no-op for `release-plz update`, leaving
# nothing to commit; `cargo publish` below still publishes the unpublished
# crates from the current tree.
if ! git diff --quiet; then
	git add -A
	git commit -m "chore(release): $tag"
	git push origin main
fi

echo "==> publishing to crates.io (cargo publish --workspace)"
cargo publish --workspace

echo "==> tagging $tag"
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
	echo "tag $tag already exists locally; skipping tag creation"
else
	git tag -a "$tag" -m "$tag"
fi
git push origin "$tag"

if command -v gh >/dev/null 2>&1 && gh auth status >/dev/null 2>&1; then
	if gh release view "$tag" >/dev/null 2>&1; then
		echo "==> GitHub release $tag already exists; skipping"
	else
		echo "==> creating GitHub release $tag (auto-generated notes)"
		gh release create "$tag" --title "$tag" --generate-notes
	fi
else
	echo "note: 'gh' unavailable or not authenticated; skipping GitHub release." >&2
	echo "      create it manually: gh release create $tag --generate-notes" >&2
fi

echo "done: released $tag"
