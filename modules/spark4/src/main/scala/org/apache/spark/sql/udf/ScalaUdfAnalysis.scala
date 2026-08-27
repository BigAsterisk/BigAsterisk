package org.apache.spark.sql.udf

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.xbean.asm9.{ClassReader, Opcodes, Type}
import org.apache.xbean.asm9.tree._

import org.bigasterisk.api.{UdfBranch, UdfPath, UdfProfile}

/**
 * Reading the inside of a Scala UDF, from its bytecode.
 *
 * ==Why bytecode==
 * A Python UDF's body is source the front end can parse. A Scala UDF arrives here as a
 * closure object; there is no source, and the logic is JVM bytecode. Reaching it takes
 * three steps, none of which involve the user doing anything:
 *
 *   1. Spark's UDFs must be serializable, so the closure has a `writeReplace` that
 *      yields a `java.lang.invoke.SerializedLambda`. That names the class and method
 *      the lambda's body was compiled into.
 *   2. That class is on the classloader, so its bytecode loads as a resource.
 *   3. The method is abstractly interpreted over a symbolic stack: parameters are
 *      symbols, constants are values, and a conditional jump forks the analysis into
 *      the branch taken and the branch not taken.
 *
 * The result is the same [[UdfProfile]] the Python analyser produces, so every consumer
 * — operation isolation, test generation, influence — works on a Scala UDF without
 * knowing which language it came from.
 *
 * ==What it reads==
 * Comparisons of a parameter against a constant, `&&`/`||`/`!` as the branch structure
 * they compile to, integer and floating-point arithmetic, `equals`, `startsWith`,
 * `endsWith`, `contains`, `length`, `toUpperCase`, `toLowerCase`, `trim`, and null
 * tests. Returns of constants are recorded so a caller can invert the function.
 *
 * ==What it refuses==
 * Loops, exception handlers, calls it does not model, and anything that leaves a value
 * on the stack it cannot describe. A refusal is recorded in the profile and the paths
 * it affects are marked inexact — never guessed at, because a wrong branch condition
 * would mis-rank an operation or generate a test that proves nothing.
 */
object ScalaUdfAnalysis {

  /** Enumerating paths is exponential in branch nesting; this bounds it. */
  private val MaxPaths = 64

  /** Analysis is pure, and a query re-analyses the same closure on every call. */
  private val cache = new ConcurrentHashMap[String, Option[UdfProfile]]()

  /**
   * The profile of `udf`, derived from its bytecode.
   *
   * `None` when the closure cannot be reached at all — not serializable, no bytecode on
   * the classloader, or an arity the analysis cannot line up with the call site.
   */
  def profile(udf: ScalaUDF): Option[UdfProfile] = {
    val name = nameOf(udf)
    cache.computeIfAbsent(key(udf), _ => derive(udf, name))
  }

  /** The name a Scala UDF appears under, which is rarely a useful one. */
  def nameOf(udf: ScalaUDF): String =
    udf.udfName.filter(_.nonEmpty).getOrElse {
      // an anonymous UDF still needs a stable name to be reported under
      s"scalaUdf${math.abs(key(udf).hashCode) % 100000}"
    }

  private def key(udf: ScalaUDF): String =
    s"${udf.function.getClass.getName}/${udf.children.size}"

  private def derive(udf: ScalaUDF, name: String): Option[UdfProfile] =
    try {
      val lambda = serializedLambda(udf.function).getOrElse(return None)
      val (owner, method) = implementation(lambda).getOrElse(return None)

      // A lambda that captured values takes them as leading parameters of the compiled
      // method. They are constants for this call site, so they are substituted rather
      // than treated as inputs.
      val captured = (0 until lambda.getCapturedArgCount).map(lambda.getCapturedArg)
      val parameters = (0 until udf.children.size).map(i => s"arg$i")

      val analysis = new MethodAnalysis(owner, method, captured, parameters)
      analysis.run()

      if (analysis.parameterCount != udf.children.size) {
        // the compiled method's arity does not line up with the call, so binding its
        // conditions to the call site's arguments would attach them to the wrong columns
        return None
      }

      Some(UdfProfile(
        name = name,
        parameters = parameters,
        branches = analysis.branches.toSeq.distinct,
        paths = analysis.paths.toSeq,
        influencing = analysis.influencing,
        unsupported = analysis.unsupported.toSeq.distinct))
    } catch {
      case NonFatal(_) => None
    }

  /** The `SerializedLambda` behind a closure, if it is one. */
  private def serializedLambda(function: AnyRef): Option[java.lang.invoke.SerializedLambda] =
    try {
      val writeReplace = function.getClass.getDeclaredMethod("writeReplace")
      writeReplace.setAccessible(true)
      writeReplace.invoke(function) match {
        case lambda: java.lang.invoke.SerializedLambda => Some(lambda)
        case _                                         => None
      }
    } catch {
      case NonFatal(_) => None
    }

  /**
   * The method node the lambda's body was compiled into.
   *
   * Scala emits an `$adapted` bridge for a lambda over primitives: it unboxes its
   * arguments and delegates. Analysing the bridge would see only boxing, so the
   * delegate is followed.
   */
  private def implementation(
      lambda: java.lang.invoke.SerializedLambda): Option[(String, MethodNode)] = {
    val owner = lambda.getImplClass
    val node = classNode(owner).getOrElse(return None)
    val methods = node.methods.asScala

    val direct = methods.find(m =>
      m.name == lambda.getImplMethodName && m.desc == lambda.getImplMethodSignature)

    val resolved = direct match {
      case Some(m) if m.name.endsWith("$adapted") =>
        val underlying = m.name.stripSuffix("$adapted")
        methods.find(_.name == underlying).orElse(Some(m))
      case other => other
    }
    resolved.map(owner -> _)
  }

  private def classNode(internalName: String): Option[ClassNode] = {
    val resource = s"/${internalName.replace('.', '/')}.class"
    val stream = Option(getClass.getResourceAsStream(resource))
      .orElse(Option(Thread.currentThread().getContextClassLoader
        .getResourceAsStream(resource.drop(1))))
    stream.map { in =>
      try {
        val node = new ClassNode()
        new ClassReader(in).accept(node, ClassReader.SKIP_FRAMES | ClassReader.SKIP_DEBUG)
        node
      } finally in.close()
    }
  }

  // ---------------------------------------------------------------------------
  // A symbolic value on the interpreter's stack
  // ---------------------------------------------------------------------------

  private sealed trait Sym
  private case class Param(index: Int) extends Sym
  private case class Const(value: Any) extends Sym
  private case class Arith(op: String, left: Sym, right: Sym) extends Sym
  private case class Apply(function: String, args: Seq[Sym]) extends Sym
  private case class Compare(op: String, left: Sym, right: Sym) extends Sym
  private case class Negated(inner: Sym) extends Sym
  private case object Opaque extends Sym

  /** Renders a symbol as SQL over the parameter names, or fails. */
  private class Renderer(parameters: Seq[String]) {
    def apply(sym: Sym): Option[String] = sym match {
      case Param(i)     => parameters.lift(i)
      case Const(v)     => literal(v)
      case Arith(op, l, r) =>
        for (a <- apply(l); b <- apply(r)) yield s"($a $op $b)"
      case Compare(op, l, r) =>
        for (a <- apply(l); b <- apply(r)) yield
          if (op == "IS NULL" || op == "IS NOT NULL") s"$a $op" else s"($a $op $b)"
      case Apply(f, args) =>
        val rendered = args.map(apply)
        if (rendered.forall(_.isDefined)) Some(s"$f(${rendered.flatten.mkString(", ")})")
        else None
      case Negated(inner) => apply(inner).map(text => s"(NOT $text)")
      case Opaque => None
    }

    private def literal(value: Any): Option[String] = value match {
      case null            => Some("NULL")
      case s: String       => Some("'" + s.replace("'", "''") + "'")
      case b: Boolean      => Some(if (b) "TRUE" else "FALSE")
      case n: java.lang.Number => Some(n.toString)
      case _               => None
    }
  }

  // ---------------------------------------------------------------------------
  // Abstract interpretation of one method
  // ---------------------------------------------------------------------------

  private class MethodAnalysis(
      owner: String,
      method: MethodNode,
      captured: Seq[Any],
      parameters: Seq[String]) {

    val branches = mutable.ArrayBuffer.empty[UdfBranch]
    val paths = mutable.ArrayBuffer.empty[UdfPath]
    val unsupported = mutable.ArrayBuffer.empty[String]
    var influencing: Set[String] = Set.empty

    private val isStatic = (method.access & Opcodes.ACC_STATIC) != 0
    private val argumentTypes = Type.getArgumentTypes(method.desc)
    private val render = new Renderer(parameters)

    /** Parameters of the *call site*, after the captured values the lambda closed over. */
    val parameterCount: Int = argumentTypes.length - captured.length

    // decisions per path, so control influence can be computed the way it is for Python
    private val decisions = mutable.ArrayBuffer.empty[(Seq[(Int, Boolean)], Option[String])]

    def run(): Unit = {
      if (parameterCount != parameters.size) {
        unsupported += s"arity mismatch: the compiled method takes $parameterCount"
        return
      }
      if (!method.tryCatchBlocks.isEmpty) {
        unsupported += "the function catches exceptions"
      }

      // Local slot -> symbol. Captured values are constants; the rest are parameters.
      val locals = mutable.Map.empty[Int, Sym]
      var slot = if (isStatic) 0 else { locals(0) = Opaque; 1 }
      argumentTypes.zipWithIndex.foreach { case (t, i) =>
        locals(slot) =
          if (i < captured.length) Const(captured(i)) else Param(i - captured.length)
        slot += t.getSize
      }

      walk(method.instructions.getFirst, Nil, locals.toMap, Nil, Set.empty)
      influencing = computeInfluence()
    }

    /**
     * Walks from `insn`, forking at every conditional jump.
     *
     * `visited` carries the jump targets already taken on this path: revisiting one is a
     * loop, which this analysis does not model, so the path stops and says so.
     */
    private def walk(
        start: AbstractInsnNode,
        constraints: List[Option[String]],
        locals: Map[Int, Sym],
        decisionsSoFar: List[(Int, Boolean)],
        visited: Set[AbstractInsnNode]): Unit = {

      if (paths.size >= MaxPaths) {
        unsupported += s"more than $MaxPaths paths"
        return
      }

      var insn = start
      var stack: List[Sym] = Nil
      var local = locals
      var seen = visited
      var advance = true

      while (insn != null) {
        advance = true

        // Checked before dispatch: a return opcode is an ordinary instruction node, and
        // letting it fall through to the arithmetic handler would discard the very value
        // being returned.
        if (isReturn(insn)) {
          val value = if (insn.getOpcode == Opcodes.RETURN) None else stack.headOption
          emit(constraints, value, decisionsSoFar, exact = true)
          return
        }

        insn match {
          case _: LabelNode | _: LineNumberNode | _: FrameNode =>

          case jump: JumpInsnNode if jump.getOpcode == Opcodes.GOTO =>
            if (seen.contains(jump.label)) {
              emit(constraints, None, decisionsSoFar, exact = false)
              unsupported += "the function loops"
              return
            }
            seen += jump.label
            insn = jump.label
            advance = false

          case jump: JumpInsnNode =>
            val (symbol, rest) = conditionOf(jump, stack)
            stack = rest
            val taken = symbol.flatMap(render.apply)
            val notTaken = symbol.map(negateSym).flatMap(render.apply)

            // `if (c) a else b` compiles to a jump on `!c` to the else, so the branch as
            // the source wrote it is the fall-through condition. Registering that one
            // keeps a branch reading the way the code does.
            val id = branchId(notTaken)

            // The JVM takes the jump when the condition it encodes holds and falls
            // through when it does not. `if (x > 1000) a else b` compiles to a jump on
            // `x <= 1000` to the *else*, so the branch reached by jumping is the one
            // where the jump's own condition is true.
            if (seen.contains(jump.label)) {
              unsupported += "the function loops"
              emit(constraints, None, decisionsSoFar, exact = false)
              return
            }

            walk(jump.label, taken :: constraints, local,
              (id, false) :: decisionsSoFar, seen + jump.label)
            walk(insn.getNext, notTaken :: constraints, local,
              (id, true) :: decisionsSoFar, seen)
            return

          case node: VarInsnNode =>
            node.getOpcode match {
              case Opcodes.ILOAD | Opcodes.LLOAD | Opcodes.FLOAD |
                   Opcodes.DLOAD | Opcodes.ALOAD =>
                stack = local.getOrElse(node.`var`, Opaque) :: stack
              case Opcodes.ISTORE | Opcodes.LSTORE | Opcodes.FSTORE |
                   Opcodes.DSTORE | Opcodes.ASTORE =>
                val (top, rest) = pop(stack)
                local = local.updated(node.`var`, top)
                stack = rest
              case _ =>
                stack = Opaque :: stack
            }

          case node: InsnNode => stack = simple(node, stack)

          case node: IntInsnNode =>
            stack = Const(node.operand) :: stack

          case node: LdcInsnNode =>
            stack = Const(node.cst) :: stack

          case node: MethodInsnNode =>
            stack = call(node, stack)

          case node: TypeInsnNode if node.getOpcode == Opcodes.CHECKCAST =>
          // a cast leaves the value alone as far as this analysis is concerned

          case other =>
            unsupported += s"opcode ${other.getOpcode} is not understood"
            emit(constraints, None, decisionsSoFar, exact = false)
            return
        }

        if (advance && insn != null) insn = insn.getNext
      }

      emit(constraints, None, decisionsSoFar, exact = true)
    }

    private def isReturn(insn: AbstractInsnNode): Boolean =
      insn.getOpcode >= Opcodes.IRETURN && insn.getOpcode <= Opcodes.RETURN

    private def pop(stack: List[Sym]): (Sym, List[Sym]) =
      stack match {
        case head :: tail => (head, tail)
        case Nil          => (Opaque, Nil)
      }

    private def simple(node: InsnNode, stack: List[Sym]): List[Sym] = {
      def arith(op: String): List[Sym] = {
        val (right, rest) = pop(stack)
        val (left, rest2) = pop(rest)
        Arith(op, left, right) :: rest2
      }
      node.getOpcode match {
        case Opcodes.ICONST_M1 => Const(-1) :: stack
        case Opcodes.ICONST_0  => Const(0) :: stack
        case Opcodes.ICONST_1  => Const(1) :: stack
        case Opcodes.ICONST_2  => Const(2) :: stack
        case Opcodes.ICONST_3  => Const(3) :: stack
        case Opcodes.ICONST_4  => Const(4) :: stack
        case Opcodes.ICONST_5  => Const(5) :: stack
        case Opcodes.LCONST_0  => Const(0L) :: stack
        case Opcodes.LCONST_1  => Const(1L) :: stack
        case Opcodes.DCONST_0  => Const(0.0) :: stack
        case Opcodes.DCONST_1  => Const(1.0) :: stack
        case Opcodes.ACONST_NULL => Const(null) :: stack

        case Opcodes.IADD | Opcodes.LADD | Opcodes.DADD | Opcodes.FADD => arith("+")
        case Opcodes.ISUB | Opcodes.LSUB | Opcodes.DSUB | Opcodes.FSUB => arith("-")
        case Opcodes.IMUL | Opcodes.LMUL | Opcodes.DMUL | Opcodes.FMUL => arith("*")
        case Opcodes.IDIV | Opcodes.LDIV | Opcodes.DDIV | Opcodes.FDIV => arith("/")

        // widening and narrowing leave the value as it is for this purpose
        case Opcodes.I2L | Opcodes.I2D | Opcodes.I2F | Opcodes.L2I | Opcodes.L2D |
             Opcodes.D2I | Opcodes.D2L | Opcodes.F2D => stack

        case Opcodes.DUP => stack.headOption.map(_ :: stack).getOrElse(stack)
        case Opcodes.POP => pop(stack)._2

        // a three-way comparison feeding an IF*: keep both sides for the jump to read
        case Opcodes.LCMP | Opcodes.DCMPG | Opcodes.DCMPL |
             Opcodes.FCMPG | Opcodes.FCMPL =>
          val (right, rest) = pop(stack)
          val (left, rest2) = pop(rest)
          Compare("cmp", left, right) :: rest2

        case _ =>
          unsupported += s"opcode ${node.getOpcode} is not understood"
          Opaque :: stack
      }
    }

    /** The string and boxing calls this models; everything else is opaque. */
    private def call(node: MethodInsnNode, stack: List[Sym]): List[Sym] = {
      val argumentCount = Type.getArgumentTypes(node.desc).length
      val (args, rest) = stack.splitAt(argumentCount)
      val arguments = args.reverse

      val receiverNeeded = node.getOpcode != Opcodes.INVOKESTATIC
      val (receiver, remaining) =
        if (receiverNeeded) pop(rest) else (Opaque, rest)

      def modelled(name: String, args: Seq[Sym]): List[Sym] =
        Apply(name, args) :: remaining

      (node.owner, node.name) match {
        case ("java/lang/String", "equals")     => Compare("=", receiver, arguments.head) :: remaining
        case ("java/lang/String", "startsWith") => modelled("startswith", receiver +: arguments)
        case ("java/lang/String", "endsWith")   => modelled("endswith", receiver +: arguments)
        case ("java/lang/String", "contains")   => modelled("contains", receiver +: arguments)
        case ("java/lang/String", "length")     => modelled("length", Seq(receiver))
        case ("java/lang/String", "toUpperCase") => modelled("upper", Seq(receiver))
        case ("java/lang/String", "toLowerCase") => modelled("lower", Seq(receiver))
        case ("java/lang/String", "trim")       => modelled("trim", Seq(receiver))
        case ("java/lang/Math", "abs")          => Apply("abs", arguments) :: remaining

        // boxing and unboxing are transparent
        case (o, "valueOf") if o.startsWith("java/lang/") && arguments.nonEmpty =>
          arguments.head :: remaining
        case (o, n) if o.startsWith("java/lang/") &&
          Set("intValue", "longValue", "doubleValue", "floatValue", "booleanValue")(n) =>
          receiver :: remaining
        case ("scala/Predef$", _) if arguments.nonEmpty =>
          arguments.head :: remaining

        case _ =>
          unsupported += s"call to ${node.owner.replace('/', '.')}.${node.name}"
          val returns = Type.getReturnType(node.desc)
          if (returns.getSort == Type.VOID) remaining else Opaque :: remaining
      }
    }

    /** The source-level condition a conditional jump encodes, and the stack after it. */
    private def conditionOf(jump: JumpInsnNode, stack: List[Sym]): (Option[Sym], List[Sym]) = {
      def binary(op: String): (Option[Sym], List[Sym]) = {
        val (right, rest) = pop(stack)
        val (left, rest2) = pop(rest)
        (Some(Compare(op, left, right)), rest2)
      }
      def unary(op: String, against: Sym): (Option[Sym], List[Sym]) = {
        val (value, rest) = pop(stack)
        value match {
          // `IFEQ` after a comparison call is `!cmp`, and after LCMP it is `a == b`
          case Compare("cmp", l, r) => (Some(Compare(op, l, r)), rest)
          case Compare(other, l, r) if op == "=" && against == Const(0) =>
            (Some(Compare(negateOp(other), l, r)), rest)
          case Compare(other, l, r) if op == "<>" && against == Const(0) =>
            (Some(Compare(other, l, r)), rest)
          case call: Apply if op == "=" && against == Const(0) =>
            (Some(Negated(call)), rest)
          case call: Apply if op == "<>" && against == Const(0) =>
            (Some(call), rest)
          case other => (Some(Compare(op, other, against)), rest)
        }
      }

      jump.getOpcode match {
        case Opcodes.IF_ICMPEQ => binary("=")
        case Opcodes.IF_ICMPNE => binary("<>")
        case Opcodes.IF_ICMPLT => binary("<")
        case Opcodes.IF_ICMPLE => binary("<=")
        case Opcodes.IF_ICMPGT => binary(">")
        case Opcodes.IF_ICMPGE => binary(">=")
        case Opcodes.IF_ACMPEQ => binary("=")
        case Opcodes.IF_ACMPNE => binary("<>")

        case Opcodes.IFEQ => unary("=", Const(0))
        case Opcodes.IFNE => unary("<>", Const(0))
        case Opcodes.IFLT => unary("<", Const(0))
        case Opcodes.IFLE => unary("<=", Const(0))
        case Opcodes.IFGT => unary(">", Const(0))
        case Opcodes.IFGE => unary(">=", Const(0))

        case Opcodes.IFNULL =>
          val (value, rest) = pop(stack)
          (Some(Compare("IS NULL", value, Const(null))), rest)
        case Opcodes.IFNONNULL =>
          val (value, rest) = pop(stack)
          (Some(Compare("IS NOT NULL", value, Const(null))), rest)

        case other =>
          unsupported += s"jump opcode $other is not understood"
          (None, stack)
      }
    }

    private def negateOp(op: String): String = op match {
      case "="  => "<>"
      case "<>" => "="
      case "<"  => ">="
      case "<=" => ">"
      case ">"  => "<="
      case ">=" => "<"
      case "IS NULL"     => "IS NOT NULL"
      case "IS NOT NULL" => "IS NULL"
      case other => other
    }

    /**
     * The opposite of a condition, as the source would have written it.
     *
     * Inverting the operator rather than wrapping the text in `NOT` matters: these
     * strings are the identity a branch is covered and ranked under, and
     * `NOT (amount <= 1000)` and `amount > 1000` would otherwise be two different
     * branches that are the same branch.
     */
    private def negateSym(sym: Sym): Sym = sym match {
      case Compare(op, l, r) => Compare(negateOp(op), l, r)
      case Negated(inner)    => inner
      case other             => Negated(other)
    }

    private def branchId(condition: Option[String]): Int = {
      condition match {
        case Some(text) =>
          val existing = branches.indexWhere(_.condition == text)
          if (existing >= 0) existing
          else {
            branches += UdfBranch(text, parameters.filter(text.contains).toSet)
            branches.size - 1
          }
        case None => -1
      }
    }

    private def emit(
        constraints: List[Option[String]],
        value: Option[Sym],
        decisionsSoFar: List[(Int, Boolean)],
        exact: Boolean): Unit = {
      if (paths.size >= MaxPaths) return

      val readable = constraints.flatten.reverse
      val complete = exact && constraints.forall(_.isDefined)
      val constraint = if (readable.isEmpty) "true" else readable.mkString(" AND ")

      val returns = value.flatMap {
        case c: Const => render(c)
        case _        => None
      }

      paths += UdfPath(constraint, returns, complete)
      decisions += (decisionsSoFar.reverse -> returns)
    }

    /**
     * Which parameters can change the result.
     *
     * Data flow through a returned value, or control over a branch whose two sides
     * return different things — the same rule the Python analysis uses, so a profile
     * means the same thing whichever language it came from.
     */
    private def computeInfluence(): Set[String] = {
      val fromControl = branches.zipWithIndex.flatMap { case (branch, id) =>
        val taken = decisions.filter(_._1.contains(id -> true)).map(_._2).toSet
        val notTaken = decisions.filter(_._1.contains(id -> false)).map(_._2).toSet
        if (taken != notTaken) branch.parameters else Set.empty[String]
      }.toSet

      // A returned constant carries no parameter; anything else is conservatively
      // treated as carrying every parameter the function could still read.
      val fromData =
        if (paths.exists(p => p.returns.isEmpty)) parameters.toSet else Set.empty[String]

      fromControl ++ fromData
    }
  }
}
