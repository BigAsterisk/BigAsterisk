"""What the tools re-run.

Several of the tools here are search procedures: fuzzing, symbolic test generation and
delta debugging all work by proposing an input, running the query with it, and looking
at what came out — dozens or hundreds of times over.

That makes "the query" not a result but a recipe, and a recipe can arrive in two forms.
A SQL string re-runs by being re-parsed, so whatever is registered under ``flights`` at
the time is what it reads. A DataFrame cannot: by the time you hold one, its plan is
analysed and bound, and ``flights`` has stopped being a name. Running it again runs it
against the same data.

Requiring the string form would mean rewriting a PySpark pipeline as SQL before you
could debug it — which is not the program you set out to debug. So the tools take
either, and the JVM side substitutes into a DataFrame's plan when it has to. That is
what :func:`as_query` hands across the boundary.
"""


def as_query(query):
    """``query`` in the form the JVM tools expect.

    A string goes over as a string; a DataFrame goes over as the underlying Java
    ``Dataset``. Anything else is refused here rather than becoming a Py4J error about
    an overload that does not exist.
    """
    if isinstance(query, str):
        return query
    jdf = getattr(query, "_jdf", None)
    if jdf is not None:
        return jdf
    raise TypeError(
        "a query must be SQL text or a DataFrame, not %s" % type(query).__name__)
