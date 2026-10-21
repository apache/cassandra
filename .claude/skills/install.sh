#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${SKILLS_DIR:-$HOME/.claude/skills}"
DRY_RUN=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [--target DIR] [--dry-run] [skill ...]

Installs skills into TARGET_DIR (default: ~/.claude/skills).
Overwrites existing skills from this repo; does not touch other skills.

Options:
  --target DIR    Install to DIR instead of ~/.claude/skills
  --dry-run       Show what would happen without copying anything
  --help          Show this help

Arguments:
  skill ...       Install only the named skill(s) (default: all)
EOF
}

SELECTED_SKILLS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)
            TARGET_DIR="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        -*)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
        *)
            SELECTED_SKILLS+=("$1")
            shift
            ;;
    esac
done

# Discover all skills in this repo (any directory containing SKILL.md)
ALL_SKILLS=()
while IFS= read -r skill_file; do
    skill_dir="$(dirname "$skill_file")"
    # Only include direct children of SCRIPT_DIR, not nested
    if [[ "$(dirname "$skill_dir")" == "$SCRIPT_DIR" ]]; then
        ALL_SKILLS+=("$(basename "$skill_dir")")
    fi
done < <(find "$SCRIPT_DIR" -name "SKILL.md" | sort)

if [[ ${#SELECTED_SKILLS[@]} -gt 0 ]]; then
    SKILLS_TO_INSTALL=("${SELECTED_SKILLS[@]}")
else
    SKILLS_TO_INSTALL=("${ALL_SKILLS[@]}")
fi

if [[ "$DRY_RUN" == false ]]; then
    mkdir -p "$TARGET_DIR"
fi

installed=0
skipped=0
for skill in "${SKILLS_TO_INSTALL[@]}"; do
    src="$SCRIPT_DIR/$skill"
    dst="$TARGET_DIR/$skill"

    if [[ ! -d "$src" ]] || [[ ! -f "$src/SKILL.md" ]]; then
        echo "warning: '$skill' not found in $SCRIPT_DIR, skipping" >&2
        ((skipped++)) || true
        continue
    fi

    if [[ "$DRY_RUN" == true ]]; then
        if [[ -d "$dst" ]]; then
            echo "would overwrite  $dst"
        else
            echo "would install    $dst"
        fi
    else
        if [[ -d "$dst" ]]; then
            rm -rf "$dst"
            echo "overwriting  $skill"
        else
            echo "installing   $skill"
        fi
        cp -r "$src" "$dst"
    fi
    ((installed++)) || true
done

echo ""
if [[ "$DRY_RUN" == true ]]; then
    echo "${installed} skill(s) would be installed to $TARGET_DIR"
    if [[ $skipped -gt 0 ]]; then echo "${skipped} skill(s) skipped (not found)"; fi
else
    echo "${installed} skill(s) installed to $TARGET_DIR"
    if [[ $skipped -gt 0 ]]; then echo "${skipped} skill(s) skipped (not found)"; fi
fi
