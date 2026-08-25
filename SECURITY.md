# Security policy

## Supported versions

BigAsterisk is a research platform under active development. Security fixes are applied
to `main`; there is no long-term support branch.

| Version | Supported |
|---|---|
| `main` | yes |
| tagged releases | latest only |

## Reporting a vulnerability

Please report vulnerabilities privately, **not** as a public issue.

- Use GitHub's [private vulnerability reporting](https://github.com/BigAsterisk/BigAsterisk/security/advisories/new), or
- email **gulzar@cs.vt.edu**.

Please include what the issue is, how to reproduce it, and what an attacker could do with
it. You can expect an acknowledgement within a week.

## Scope worth knowing about

Some of what this project does is inherently powerful, and is not a vulnerability in
itself:

- **Tools evaluate user-supplied SQL and expressions.** Predicates, oracles and
  distribution declarations are executed. Treat them as code, because they are.
- **Fuzzing and test generation register temporary views under the query's own table
  names** while a campaign runs, and restore the originals afterwards. Do not point them
  at a session whose views matter to something else concurrently.
- **Captured records reach the driver.** Watchpoints, profiles and crash-culprit guards
  bring sampled rows back. If your data is sensitive, the capacity limits are the control.

A report that these are possible is not a vulnerability. A report that one of them
escapes its documented bounds is.
