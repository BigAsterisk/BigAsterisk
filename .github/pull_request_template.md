## What this changes

<!-- One or two sentences. If it fixes an issue, link it. -->

## Why

<!-- The problem, not the patch. If a paper describes the behaviour, cite the section. -->

## Checklist

- [ ] A test that fails without this change
- [ ] `bin/sbt test` passes
- [ ] `python/tests/run.sh` passes, if the change reaches the PySpark surface
- [ ] `bin/bigasterisk tour` is green
- [ ] `scripts/validate-notebooks.sh` is green, if the change reaches a notebook
- [ ] Docs updated, including anything this changes about what a tool does **not** do
- [ ] `PROVENANCE.md` updated, if this moves the line on what was reproduced

## Anything surprising you found

<!-- Optional, and genuinely useful. Several of the sharpest bugs here were found by a
     test written before the fix, and the commit messages record what the test revealed. -->
