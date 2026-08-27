#!/usr/bin/env python3
"""Fail if a committed notebook carries stored outputs.

Outputs do not belong in the repository: they bloat every diff, and they carry whatever
paths, hostnames and timings the machine that ran them happened to have. The
documentation site needs them, which is what scripts/render-notebook-page.py is for —
it generates a page from a run rather than committing the run.

    scripts/check-notebooks-clean.py [--fix]
"""

import glob
import json
import sys

def main():
    fix = "--fix" in sys.argv
    dirty = []

    for path in sorted(glob.glob("notebooks/*.ipynb")):
        with open(path) as handle:
            notebook = json.load(handle)

        outputs = sum(len(c.get("outputs", [])) for c in notebook["cells"])
        counts = sum(1 for c in notebook["cells"] if c.get("execution_count"))
        if not outputs and not counts:
            continue

        dirty.append((path, outputs, counts))
        if fix:
            for cell in notebook["cells"]:
                if cell["cell_type"] == "code":
                    cell["outputs"] = []
                    cell["execution_count"] = None
            with open(path, "w") as handle:
                json.dump(notebook, handle, indent=1)
                handle.write("\n")

    if not dirty:
        print("notebooks are clean: no stored outputs")
        return 0

    for path, outputs, counts in dirty:
        print(f"{'cleaned' if fix else 'has output'}: {path} "
              f"({outputs} outputs, {counts} execution counts)")
    if fix:
        return 0

    print("\nRun scripts/check-notebooks-clean.py --fix, then commit.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
