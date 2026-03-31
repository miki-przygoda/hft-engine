#!/usr/bin/env bash
# PostToolUse hook — fires after Write or Edit tool calls.
# If the written file is not listed in the ### File map section of CLAUDE.md,
# injects an additionalContext message so Claude updates the map.

PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null)
[ -z "$PROJECT_ROOT" ] && exit 0

CLAUDE_MD="$PROJECT_ROOT/CLAUDE.md"

INPUT=$(cat)
FILE_PATH=$(printf '%s' "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null)

# Nothing to do if no file path in the tool call
[ -z "$FILE_PATH" ] && exit 0

# Make relative to project root
REL_PATH="${FILE_PATH#$PROJECT_ROOT/}"

# If stripping the prefix didn't change the path, the file is outside the project
[[ "$REL_PATH" == "$FILE_PATH" ]] && exit 0

# Skip files that don't belong in the file map
[[ "$REL_PATH" == "CLAUDE.md" ]]          && exit 0
[[ "$REL_PATH" == "README.md" ]]          && exit 0
[[ "$REL_PATH" == .claude/* ]]            && exit 0
[[ "$REL_PATH" == *.lock ]]               && exit 0
[[ "$REL_PATH" == *.toml ]]               && exit 0
[[ "$REL_PATH" == *.json ]]               && exit 0
[[ "$REL_PATH" == .gitignore ]]           && exit 0

[ ! -f "$CLAUDE_MD" ] && exit 0

FILENAME=$(basename "$FILE_PATH")

# Check if the filename appears anywhere inside the File map code block
IN_MAP=$(awk '
  /^### File map/ { in_section=1; next }
  in_section && /^```/ { if (in_block) exit; in_block=1; next }
  in_block { print }
' "$CLAUDE_MD" | grep -cF "$FILENAME" 2>/dev/null || true)

if [ "$IN_MAP" -eq 0 ]; then
    printf '{"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":"FILE MAP UPDATE NEEDED: \"%s\" was just written/edited but is not listed in the ### File map section of CLAUDE.md. Update the file map now — add the file path with a short inline comment describing its purpose."}}\n' "$REL_PATH"
fi

exit 0
