# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Reading the inside of a Python UDF.

A UDF is opaque to plan analysis. Every technique that reasons about branches — which
operation is at fault, which input reaches which path, which column actually influenced
a result — stops at its boundary and treats the whole call as one black box over all of
its arguments.

This crosses that boundary for Python UDFs, by parsing the function's own source:

    import bigasterisk

    def classify(amount):
        if amount > 1000:
            return "high"
        elif amount > 100:
            return "medium"
        return "low"

    bigasterisk.udf.register(spark, classify)

From then on the JVM engines know that ``classify`` has two branches over ``amount``,
three paths, that ``amount`` influences the result, and — because the paths are exact —
that ``classify(amount) = 'high'`` means ``amount > 1000``. Test generation solves
through it, operation-level fault localisation ranks its branches separately, and
influence-based provenance names the columns that really mattered.

What it reads
-------------
``if``/``elif``/``else`` over comparisons of parameters against literals, ``and``/``or``/
``not``, ``is None``, ``in``, arithmetic with ``+ - * /``, ``len``/``abs``/``upper``/
``lower``/``strip``, ``startswith``/``endswith``/``in`` for strings, conditional
expressions in a ``return``, assignments to locals, and free variables that resolve to
constants.

Anything else is reported rather than guessed at: the profile lists what it could not
read, the affected paths are marked inexact, and every consumer degrades to treating
the function as a black box for those paths. A wrong branch condition would silently
mis-rank an operation or generate a test that proves nothing, so nothing is assumed.

Only row-at-a-time Python UDFs are analysed. A pandas UDF receives a Series rather than
a value, so the same source means something different; those are refused by name.
"""

import ast
import inspect
import textwrap

__all__ = ["analyze", "register", "unregister", "registered", "UdfProfile", "Unsupported"]


# Enumerating paths is exponential in branch nesting. This bounds it; a function that
# exceeds it is profiled as far as the bound and says so.
MAX_PATHS = 64


class Unsupported(Exception):
    """Raised internally when an expression has no faithful SQL rendering."""


class UdfProfile(object):
    """What analysis learned about a function. Mirrors the Scala ``UdfProfile``."""

    def __init__(self, name, parameters, branches, paths, influencing, unsupported):
        self.name = name
        self.parameters = parameters
        self.branches = branches          # list of (condition, [params])
        self.paths = paths                # list of (constraint, returns_or_None, exact)
        self.influencing = influencing    # set of parameter names
        self.unsupported = unsupported    # list of reasons

    @property
    def complete(self):
        """True when the analysis read the whole function."""
        return not self.unsupported

    @property
    def solvable(self):
        """True when every path is exact, so a caller can solve through the function."""
        return self.complete and bool(self.paths) and all(p[2] for p in self.paths)

    def lines(self):
        """The wire format the JVM registry reads. See ``UdfProfile.parse`` in Scala."""

        def clean(text):
            # tabs and newlines are the record separators; conditions never need them
            return " ".join(str(text).split())

        out = ["udf\t%s\t%s" % (clean(self.name), ",".join(self.parameters))]
        for condition, params in self.branches:
            out.append("branch\t%s\t%s" % (clean(condition), ",".join(sorted(params))))
        for constraint, returns, exact in self.paths:
            out.append("path\t%s\t%s\t%s" % (
                clean(constraint), clean(returns) if returns is not None else "",
                "exact" if exact else "approximate"))
        for param in sorted(self.influencing):
            out.append("influence\t%s" % param)
        for reason in self.unsupported:
            out.append("unsupported\t%s" % clean(reason))
        return out

    def __repr__(self):
        if self.solvable:
            state = "solvable"
        elif self.complete:
            state = "complete"
        else:
            state = "partial (%d unread)" % len(self.unsupported)
        return "UdfProfile(%s(%s), %d branches, %d paths, %s)" % (
            self.name, ", ".join(self.parameters), len(self.branches),
            len(self.paths), state)


# ---------------------------------------------------------------------------
# Rendering Python expressions as SQL over the parameter names
# ---------------------------------------------------------------------------

_COMPARISONS = {
    ast.Eq: "=", ast.NotEq: "<>", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">=",
}

# `//`, `%` and `**` are deliberately absent: Spark and Python disagree about them on
# negative operands, and a constraint that is subtly wrong is worse than one that is
# missing.
_ARITHMETIC = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/"}

_ONE_ARG_FUNCTIONS = {"len": "length", "abs": "abs"}
_METHODS = {"upper": "upper", "lower": "lower", "strip": "trim"}


class _Renderer(object):
    """Turns a Python expression into SQL text, or refuses to.

    Every rendering also reports the parameters it read, which is what taint is
    computed from. Locals resolve through ``env``, so an assignment's right-hand side
    carries its parameters forward into any condition that uses it.
    """

    def __init__(self, parameters, constants, env):
        self.parameters = parameters
        self.constants = constants   # free variables that resolved to constants
        self.env = env               # local name -> (sql, params)

    def render(self, node):
        """Returns ``(sql_text, parameters_read)``."""
        method = getattr(self, "_" + type(node).__name__, None)
        if method is None:
            raise Unsupported("%s is not understood" % type(node).__name__)
        return method(node)

    def boolean(self, node):
        """Renders a node used as a condition, refusing Python truthiness.

        ``if amount:`` is true for any non-zero number and any non-empty string; SQL has
        no such coercion, so rendering it as a predicate would change its meaning.
        """
        if isinstance(node, (ast.Compare, ast.BoolOp)):
            return self.render(node)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return self.render(node)
        if isinstance(node, ast.Constant) and isinstance(node.value, bool):
            return self.render(node)
        if isinstance(node, ast.Call):
            # startswith/endswith and friends already produce a boolean
            sql, params = self.render(node)
            if sql.startswith(("startswith(", "endswith(", "contains(")):
                return sql, params
        raise Unsupported(
            "a condition must be a comparison; Python truthiness has no SQL equivalent")

    # -- leaves ------------------------------------------------------------

    def _Constant(self, node):
        return _literal(node.value), set()

    def _Name(self, node):
        if node.id in self.env:
            sql, params = self.env[node.id]
            return "(%s)" % sql, set(params)
        if node.id in self.parameters:
            return node.id, {node.id}
        if node.id in self.constants:
            return _literal(self.constants[node.id]), set()
        raise Unsupported("'%s' is not a parameter, a local, or a constant" % node.id)

    # -- operators ---------------------------------------------------------

    def _Compare(self, node):
        parts, params = [], set()
        left = node.left
        for op, right in zip(node.ops, node.comparators):
            parts.append(self._one_comparison(left, op, right, params))
            left = right
        return parts[0] if len(parts) == 1 else "(%s)" % " AND ".join(parts), params

    def _one_comparison(self, left, op, right, params):
        # `x is None` and `x == None` are null tests, not comparisons
        if isinstance(op, (ast.Is, ast.IsNot)) or _is_none(right):
            if not _is_none(right):
                raise Unsupported("'is' is only understood against None")
            sql, used = self.render(left)
            params |= used
            return "%s IS %sNULL" % (sql, "" if isinstance(op, (ast.Is, ast.Eq)) else "NOT ")

        if isinstance(op, (ast.In, ast.NotIn)):
            return self._membership(left, op, right, params)

        symbol = _COMPARISONS.get(type(op))
        if symbol is None:
            raise Unsupported("comparison '%s' is not understood" % type(op).__name__)
        left_sql, used = self.render(left)
        params |= used
        right_sql, used = self.render(right)
        params |= used
        return "%s %s %s" % (left_sql, symbol, right_sql)

    def _membership(self, left, op, right, params):
        negate = "NOT " if isinstance(op, ast.NotIn) else ""
        if isinstance(right, (ast.Tuple, ast.List, ast.Set)):
            values = []
            for element in right.elts:
                sql, used = self.render(element)
                params |= used
                values.append(sql)
            left_sql, used = self.render(left)
            params |= used
            return "%s %sIN (%s)" % (left_sql, negate, ", ".join(values))
        # `"x" in name` is a substring test
        left_sql, used = self.render(left)
        params |= used
        right_sql, used = self.render(right)
        params |= used
        return "%scontains(%s, %s)" % (negate, right_sql, left_sql)

    def _BoolOp(self, node):
        joiner = " AND " if isinstance(node.op, ast.And) else " OR "
        parts, params = [], set()
        for value in node.values:
            sql, used = self.boolean(value)
            params |= used
            parts.append(sql)
        return "(%s)" % joiner.join(parts), params

    def _UnaryOp(self, node):
        if isinstance(node.op, ast.Not):
            sql, params = self.boolean(node.operand)
            return "(NOT %s)" % sql, params
        if isinstance(node.op, ast.USub):
            sql, params = self.render(node.operand)
            return "(- %s)" % sql, params
        raise Unsupported("unary '%s' is not understood" % type(node.op).__name__)

    def _BinOp(self, node):
        symbol = _ARITHMETIC.get(type(node.op))
        if symbol is None:
            raise Unsupported(
                "'%s' is not understood; Spark and Python disagree about it on negative "
                "operands" % type(node.op).__name__)
        left, params = self.render(node.left)
        right, used = self.render(node.right)
        return "(%s %s %s)" % (left, symbol, right), params | used

    def _Call(self, node):
        params = set()
        if isinstance(node.func, ast.Name):
            name = _ONE_ARG_FUNCTIONS.get(node.func.id)
            if name is None or len(node.args) != 1:
                raise Unsupported("call to '%s' is not understood" % _describe(node.func))
            sql, used = self.render(node.args[0])
            return "%s(%s)" % (name, sql), params | used

        if isinstance(node.func, ast.Attribute):
            receiver, used = self.render(node.func.value)
            params |= used
            method = node.func.attr
            if method in _METHODS and not node.args:
                return "%s(%s)" % (_METHODS[method], receiver), params
            if method in ("startswith", "endswith") and len(node.args) == 1:
                argument, used = self.render(node.args[0])
                return "%s(%s, %s)" % (method, receiver, argument), params | used
            raise Unsupported("method '%s' is not understood" % method)

        raise Unsupported("call to %s is not understood" % _describe(node.func))

    def _IfExp(self, node):
        raise Unsupported("a conditional expression is only understood in a return")


def _literal(value):
    """A Python constant as SQL text."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return "'%s'" % value.replace("'", "''")
    if isinstance(value, (int, float)):
        return repr(value)
    raise Unsupported("a %s literal has no SQL form" % type(value).__name__)


def _is_none(node):
    return isinstance(node, ast.Constant) and node.value is None


def _describe(node):
    try:
        return ast.unparse(node)
    except Exception:                                    # pragma: no cover - old Python
        return type(node).__name__


# ---------------------------------------------------------------------------
# Walking the function
# ---------------------------------------------------------------------------

class _Walker(object):
    """Enumerates the paths through a function body.

    A path is the conjunction of the branch outcomes leading to one ``return``. An
    ``if`` is explored by walking its body and its ``else`` *each followed by whatever
    comes after it*, which is what makes the enumeration path-sensitive rather than a
    per-statement approximation.
    """

    def __init__(self, parameters, constants):
        self.parameters = parameters
        self.constants = constants
        self.branches = []        # (condition, params), in source order
        self.paths = []           # (constraint, returns, exact, decisions)
        self.unsupported = []
        self.truncated = False

    def walk(self, body, constraints, env, decisions):
        if len(self.paths) >= MAX_PATHS:
            self.truncated = True
            return

        for index, statement in enumerate(body):
            if isinstance(statement, ast.Return):
                self._emit(constraints, statement.value, env, decisions)
                return

            if isinstance(statement, ast.If):
                rest = body[index + 1:]
                self._branch(statement, rest, constraints, env, decisions)
                return

            if isinstance(statement, ast.Assign):
                self._assign(statement, env)
                continue

            if isinstance(statement, (ast.Pass, ast.Expr)) and _is_docstring(statement):
                continue

            self.unsupported.append(
                "%s is not understood" % type(statement).__name__)
            # everything from here on is unreadable, so the path stops being exact
            self._emit(constraints, None, env, decisions, exact=False)
            return

        # falling off the end of a Python function returns None
        self._emit(constraints, ast.Constant(value=None), env, decisions)

    def _branch(self, statement, rest, constraints, env, decisions):
        renderer = _Renderer(self.parameters, self.constants, env)
        identifier = len(self.branches)
        try:
            condition, params = renderer.boolean(statement.test)
        except Unsupported as failure:
            self.unsupported.append(
                "condition '%s': %s" % (_describe(statement.test), failure))
            # The branch is real but unreadable: explore both sides so the returns are
            # still enumerated, with no constraint recorded and exactness withdrawn.
            self.walk(list(statement.body) + list(rest), constraints + [None],
                      dict(env), decisions)
            self.walk(list(statement.orelse) + list(rest), constraints + [None],
                      dict(env), decisions)
            return

        self.branches.append((condition, params))
        self.walk(list(statement.body) + list(rest),
                  constraints + [condition], dict(env), decisions + [(identifier, True)])
        self.walk(list(statement.orelse) + list(rest),
                  constraints + ["(NOT %s)" % condition], dict(env),
                  decisions + [(identifier, False)])

    def _assign(self, statement, env):
        renderer = _Renderer(self.parameters, self.constants, env)
        targets = [t for t in statement.targets if isinstance(t, ast.Name)]
        if len(targets) != len(statement.targets):
            self.unsupported.append("only assignment to a plain name is understood")
            return
        try:
            sql, params = renderer.render(statement.value)
        except Unsupported as failure:
            self.unsupported.append(
                "assignment '%s': %s" % (_describe(statement.value), failure))
            for target in targets:
                env.pop(target.id, None)      # the name now holds something unreadable
            return
        for target in targets:
            env[target.id] = (sql, params)

    def _emit(self, constraints, value, env, decisions, exact=True):
        if len(self.paths) >= MAX_PATHS:
            self.truncated = True
            return

        # a `return x if c else y` is two paths, not one
        if isinstance(value, ast.IfExp):
            renderer = _Renderer(self.parameters, self.constants, env)
            identifier = len(self.branches)
            try:
                condition, params = renderer.boolean(value.test)
                self.branches.append((condition, params))
                self._emit(constraints + [condition], value.body, env,
                           decisions + [(identifier, True)], exact)
                self._emit(constraints + ["(NOT %s)" % condition], value.orelse, env,
                           decisions + [(identifier, False)], exact)
                return
            except Unsupported as failure:
                self.unsupported.append(
                    "condition '%s': %s" % (_describe(value.test), failure))
                exact = False

        returns, returned_params = None, set()
        if value is not None:
            renderer = _Renderer(self.parameters, self.constants, env)
            try:
                rendered, returned_params = renderer.render(value)
                # only a literal is usable for inverting the call; an expression still
                # marks a path, it just cannot be solved backwards
                if isinstance(value, ast.Constant):
                    returns = rendered
            except Unsupported as failure:
                self.unsupported.append(
                    "return '%s': %s" % (_describe(value), failure))

        readable = [c for c in constraints if c is not None]
        exact = exact and len(readable) == len(constraints)
        constraint = " AND ".join(readable) if readable else "true"
        self.paths.append((constraint, returns, exact, decisions, returned_params))


def _is_docstring(statement):
    return (isinstance(statement, ast.Pass) or
            (isinstance(statement, ast.Expr) and
             isinstance(statement.value, ast.Constant) and
             isinstance(statement.value.value, str)))


# ---------------------------------------------------------------------------
# Taint
# ---------------------------------------------------------------------------

def _influencing(walker, parameters):
    """The parameters that can change what the function returns.

    Two ways one can: it flows into a returned value, or it decides a branch whose two
    sides return different things. The second is why a condition matters at all — a
    parameter that only appears in a branch where both sides return the same value
    provably cannot change the result, and reporting it would be the over-approximation
    this analysis exists to avoid.
    """
    influencing = set()

    for path in walker.paths:
        influencing |= (path[4] & set(parameters))

    for identifier, (_, params) in enumerate(walker.branches):
        taken, not_taken = set(), set()
        for constraint, returns, _exact, decisions, _params in walker.paths:
            outcome = dict(decisions).get(identifier)
            if outcome is True:
                taken.add(returns)
            elif outcome is False:
                not_taken.add(returns)
        if taken != not_taken:
            influencing |= (params & set(parameters))

    return influencing


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

def _unwrap(function):
    """The plain Python function behind whatever the caller passed, and its name."""
    name = None
    # pyspark.sql.functions.udf returns a UserDefinedFunction wrapping the callable
    if hasattr(function, "func") and callable(getattr(function, "func")):
        name = getattr(function, "_name", None)
        function = function.func
    if hasattr(function, "__wrapped__"):
        function = function.__wrapped__
    if name is None:
        name = getattr(function, "__name__", None)
    if name is None:
        raise ValueError("cannot determine the UDF's name; pass name= explicitly")
    return function, name


def _parse(function):
    """The function's AST, with any decorators removed."""
    try:
        source = textwrap.dedent(inspect.getsource(function))
    except (OSError, TypeError) as failure:
        raise ValueError(
            "cannot read the source of %r (%s). A UDF defined in a REPL or built by "
            "exec() has no source to analyse." % (function, failure))

    try:
        tree = ast.parse(source)
    except SyntaxError:
        # a lambda or a def written as part of a larger expression: parse the whole
        # statement and pick the function out of it
        tree = ast.parse(source.strip().rstrip(","))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            return node
    raise ValueError("no function definition found in the source of %r" % function)


def _constants(function):
    """Free variables that resolve to constants, so `amount > THRESHOLD` is readable."""
    resolved = {}
    scalars = (int, float, str, bool, type(None))

    closure = {}
    if getattr(function, "__closure__", None):
        names = function.__code__.co_freevars
        for name, cell in zip(names, function.__closure__):
            try:
                closure[name] = cell.cell_contents
            except ValueError:                            # cell not yet filled
                continue

    for name in getattr(function.__code__, "co_names", ()):
        if name in function.__globals__:
            value = function.__globals__[name]
            if isinstance(value, scalars):
                resolved[name] = value
    for name, value in closure.items():
        if isinstance(value, scalars):
            resolved[name] = value
    return resolved


def analyze(function, name=None):
    """Analyses ``function`` and returns its :class:`UdfProfile`.

    Accepts a plain Python function or a ``pyspark.sql.functions.udf`` wrapping one.
    Raises ``ValueError`` if the source cannot be read at all; a function whose *body*
    is only partly readable produces a profile that says which parts, rather than
    failing.
    """
    function, derived = _unwrap(function)
    name = name or derived

    if getattr(function, "evalType", None) is not None:
        raise ValueError("only row-at-a-time Python UDFs are analysed")

    node = _parse(function)
    arguments = node.args
    if arguments.vararg or arguments.kwarg:
        raise ValueError("a UDF with *args or **kwargs has no fixed parameter list")
    parameters = [a.arg for a in list(arguments.posonlyargs) + list(arguments.args)]

    walker = _Walker(parameters, _constants(function))
    body = node.body if isinstance(node.body, list) else [ast.Return(value=node.body)]
    walker.walk(body, [], {}, [])

    unsupported = list(walker.unsupported)
    if walker.truncated:
        unsupported.append(
            "more than %d paths; the rest were not enumerated" % MAX_PATHS)

    return UdfProfile(
        name=name,
        parameters=parameters,
        branches=[(condition, params) for condition, params in walker.branches],
        paths=[(constraint, returns, exact)
               for constraint, returns, exact, _decisions, _params in walker.paths],
        influencing=_influencing(walker, parameters),
        unsupported=unsupported)


def register(spark, function, name=None):
    """Analyses ``function`` and makes the result available to the JVM engines.

    ``name`` must be the name the *query* uses. For ``spark.udf.register("f", fn)``
    that is ``"f"``; for a UDF applied through the DataFrame API it is the Python
    function's own name, which is the default.

    Returns the profile, so a caller can see what was read:

        profile = bigasterisk.udf.register(spark, classify)
        print(profile)          # UdfProfile(classify(amount), 2 branches, 3 paths, solvable)
        print(profile.unsupported)
    """
    profile = analyze(function, name)
    gateway = spark.sparkContext._gateway
    lines = gateway.new_array(gateway.jvm.java.lang.String, len(profile.lines()))
    for index, line in enumerate(profile.lines()):
        lines[index] = line
    spark._jvm.org.bigasterisk.api.UdfRegistry.registerLines(lines)
    return profile


def unregister(spark, name):
    """Forgets the profile registered under ``name``."""
    spark._jvm.org.bigasterisk.api.UdfRegistry.remove(name)


def registered(spark):
    """The names that currently have a profile."""
    return {str(n) for n in spark._jvm.org.bigasterisk.api.UdfRegistry.registeredNames()}
