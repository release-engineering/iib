#!/bin/bash
# Create .claude/skills/ symlinks to .agents/skills/ for Claude Code discovery.
# Run this manually after cloning the repo or if symlinks are missing.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
AGENTS_DIR="$REPO_ROOT/.agents/skills"
CLAUDE_DIR="$REPO_ROOT/.claude/skills"

if [ ! -d "$AGENTS_DIR" ]; then
    echo "Error: $AGENTS_DIR does not exist." >&2
    exit 1
fi

mkdir -p "$CLAUDE_DIR"

linked_count=0
for skill_dir in "$AGENTS_DIR"/*/; do
    [ -d "$skill_dir" ] || continue
    skill_name="$(basename "$skill_dir")"
    if [ -f "$skill_dir/SKILL.md" ]; then
        rm -rf "$CLAUDE_DIR/$skill_name"
        ln -sf "../../.agents/skills/$skill_name" "$CLAUDE_DIR/$skill_name"
        echo "Linked: .claude/skills/$skill_name -> .agents/skills/$skill_name"
        linked_count=$((linked_count + 1))
    fi
done

if [ "$linked_count" -eq 0 ]; then
    echo "Warning: no skills found in $AGENTS_DIR" >&2
    exit 1
fi

echo "Done. Linked $linked_count skill(s) for Claude Code, Cursor, VS Code Copilot, and other agents."
