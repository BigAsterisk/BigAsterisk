# Unit tests for the Python UDF analyser. No Spark: this is pure source analysis.
#
# Run through python/tests/run.sh, or directly:
#
#   PYTHONPATH=python python3 python/tests/test_udf_analysis.py

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from bigasterisk import udf as udf_analysis          # noqa: E402

passed = []


def check(name, cond, detail=""):
    if cond:
        passed.append(name)
        print("PASS  %s" % name)
    else:
        print("FAIL  %s  %s" % (name, detail))
        sys.exit(1)


# --- the functions under analysis ------------------------------------------

def classify(amount):
    if amount > 1000:
        return "high"
    elif amount > 100:
        return "medium"
    return "low"


THRESHOLD = 500


def vip(amount, cid):
    if amount > THRESHOLD and cid == "c1":
        return "vip"
    return "normal"


def ignores_its_branch(a, b):
    if a > 5:
        return "x"
    return "x"


def passes_through(amount, note):
    return amount * 2


def uses_a_local(amount):
    doubled = amount * 2
    if doubled > 100:
        return "big"
    return "small"


def handles_null(name):
    if name is None:
        return "missing"
    if name.startswith("A"):
        return "a-list"
    return "other"


def falls_off_the_end(amount):
    if amount > 10:
        return "big"


def unreadable(text):
    if text.encode("utf8") == b"x":
        return 1
    return 2


def truthiness(amount):
    if amount:
        return "set"
    return "unset"


def modulo(amount):
    if amount % 2 == 0:
        return "even"
    return "odd"


def ternary(amount):
    return "big" if amount > 10 else "small"


def in_a_set(cid):
    if cid in ("c1", "c2"):
        return "known"
    return "unknown"


# --- branches and paths ----------------------------------------------------

profile = udf_analysis.analyze(classify)
check("the parameters are read in order", profile.parameters == ["amount"])
check("both branches are found",
      [c for c, _ in profile.branches] == ["amount > 1000", "amount > 100"],
      profile.branches)
check("every path is enumerated", len(profile.paths) == 3, profile.paths)
check("a path carries what it returns",
      ("amount > 1000", "'high'", True) in profile.paths, profile.paths)
check("an inner path carries the outer negation",
      ("(NOT amount > 1000) AND amount > 100", "'medium'", True) in profile.paths,
      profile.paths)
check("a fully read function is solvable", profile.solvable)
check("nothing is reported as unread", profile.unsupported == [], profile.unsupported)

profile = udf_analysis.analyze(falls_off_the_end)
check("falling off the end is a path returning null",
      ("(NOT amount > 10)", "NULL", True) in profile.paths, profile.paths)

profile = udf_analysis.analyze(ternary)
check("a conditional expression in a return is two paths",
      len(profile.paths) == 2 and profile.solvable, profile.paths)

profile = udf_analysis.analyze(in_a_set)
check("membership becomes IN",
      profile.branches[0][0] == "cid IN ('c1', 'c2')", profile.branches)

profile = udf_analysis.analyze(handles_null)
check("a None test becomes IS NULL",
      profile.branches[0][0] == "name IS NULL", profile.branches)
check("startswith is kept as a string predicate",
      profile.branches[1][0] == "startswith(name, 'A')", profile.branches)

# --- resolving what the function reads from outside itself -----------------

profile = udf_analysis.analyze(vip)
check("a module-level constant is resolved",
      profile.branches[0][0] == "(amount > 500 AND cid = 'c1')", profile.branches)

profile = udf_analysis.analyze(uses_a_local)
check("a local carries its parameters into the condition",
      profile.branches[0][1] == {"amount"}, profile.branches)
check("a local's value is substituted",
      "amount * 2" in profile.branches[0][0], profile.branches)


def make_closure(limit):
    def over(amount):
        if amount > limit:
            return "over"
        return "under"
    return over


profile = udf_analysis.analyze(make_closure(42))
check("a closed-over constant is resolved",
      profile.branches[0][0] == "amount > 42", profile.branches)

# --- taint -----------------------------------------------------------------

profile = udf_analysis.analyze(classify)
check("a parameter that reaches the branches influences the result",
      profile.influencing == {"amount"}, profile.influencing)

profile = udf_analysis.analyze(passes_through)
check("an argument the function never reads does not influence it",
      profile.influencing == {"amount"}, profile.influencing)

profile = udf_analysis.analyze(ignores_its_branch)
check("a branch whose arms return the same value influences nothing",
      profile.influencing == set(), profile.influencing)

profile = udf_analysis.analyze(vip)
check("every parameter of a compound condition influences the result",
      profile.influencing == {"amount", "cid"}, profile.influencing)

# --- what it refuses to read -----------------------------------------------

profile = udf_analysis.analyze(unreadable)
check("an unreadable condition is reported", len(profile.unsupported) == 1,
      profile.unsupported)
check("an unreadable condition makes its paths inexact",
      all(not exact for _, _, exact in profile.paths), profile.paths)
check("an unreadable function is never solvable", not profile.solvable)
check("the returns behind an unreadable condition are still enumerated",
      len(profile.paths) == 2, profile.paths)

profile = udf_analysis.analyze(truthiness)
check("Python truthiness is refused rather than rendered",
      not profile.complete and "truthiness" in " ".join(profile.unsupported),
      profile.unsupported)

profile = udf_analysis.analyze(modulo)
check("an operator Spark and Python disagree about is refused",
      not profile.complete, profile.unsupported)

# --- the wire format -------------------------------------------------------

lines = udf_analysis.analyze(classify).lines()
check("the profile starts with its udf line", lines[0] == "udf\tclassify\tamount", lines[0])
check("every line is tab separated and single line",
      all("\n" not in line and line.count("\t") >= 1 for line in lines), lines)
check("branches, paths and influence are all emitted",
      sum(1 for line in lines if line.startswith("branch\t")) == 2 and
      sum(1 for line in lines if line.startswith("path\t")) == 3 and
      "influence\tamount" in lines, lines)

# --- unwrapping and failure modes ------------------------------------------

try:
    udf_analysis.analyze(len)
    check("a builtin is refused", False, "no error raised")
except ValueError as failure:
    check("a builtin has no source to read", "source" in str(failure), str(failure))


def variadic(*args):
    return args[0]


try:
    udf_analysis.analyze(variadic)
    check("*args is refused", False, "no error raised")
except ValueError as failure:
    check("a variadic UDF has no fixed parameter list",
          "fixed parameter list" in str(failure), str(failure))

profile = udf_analysis.analyze(classify, name="banded")
check("the registered name can differ from the function's",
      profile.name == "banded", profile.name)

print("\n%d checks passed" % len(passed))
