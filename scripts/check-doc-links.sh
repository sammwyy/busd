#!/usr/bin/env bash
# Validate local Markdown links in project documentation without a network dependency.
set -euo pipefail

failed=0

while IFS= read -r -d '' file; do
    directory=$(dirname "$file")

    while IFS= read -r target; do
        case "$target" in
            '' | \#* | http://* | https://* | mailto:* | tel:*)
                continue
                ;;
        esac

        target=${target%%\#*}
        if [[ ! -e "$directory/$target" ]]; then
            printf 'broken documentation link: %s -> %s\n' "$file" "$target" >&2
            failed=1
        fi
    done < <(sed -nE 's/.*\]\(([^ )]+).*/\1/p' "$file")
done < <(find docs -type f -name '*.md' -print0)

exit "$failed"
