package org.apache.beam.rewriter.common;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Set;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.internal.lang.Nullable;
import org.openrewrite.java.JavaVisitor;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.J.AnnotatedType;
import org.openrewrite.java.tree.J.Annotation;
import org.openrewrite.java.tree.J.ArrayAccess;
import org.openrewrite.java.tree.J.ArrayDimension;
import org.openrewrite.java.tree.J.ArrayType;
import org.openrewrite.java.tree.J.Assert;
import org.openrewrite.java.tree.J.Assignment;
import org.openrewrite.java.tree.J.AssignmentOperation;
import org.openrewrite.java.tree.J.Binary;
import org.openrewrite.java.tree.J.Block;
import org.openrewrite.java.tree.J.Break;
import org.openrewrite.java.tree.J.Case;
import org.openrewrite.java.tree.J.ClassDeclaration;
import org.openrewrite.java.tree.J.CompilationUnit;
import org.openrewrite.java.tree.J.Continue;
import org.openrewrite.java.tree.J.ControlParentheses;
import org.openrewrite.java.tree.J.DoWhileLoop;
import org.openrewrite.java.tree.J.Empty;
import org.openrewrite.java.tree.J.EnumValue;
import org.openrewrite.java.tree.J.EnumValueSet;
import org.openrewrite.java.tree.J.FieldAccess;
import org.openrewrite.java.tree.J.ForEachLoop;
import org.openrewrite.java.tree.J.ForEachLoop.Control;
import org.openrewrite.java.tree.J.ForLoop;
import org.openrewrite.java.tree.J.If;
import org.openrewrite.java.tree.J.If.Else;
import org.openrewrite.java.tree.J.Import;
import org.openrewrite.java.tree.J.InstanceOf;
import org.openrewrite.java.tree.J.Label;
import org.openrewrite.java.tree.J.Lambda;
import org.openrewrite.java.tree.J.Literal;
import org.openrewrite.java.tree.J.MemberReference;
import org.openrewrite.java.tree.J.MethodDeclaration;
import org.openrewrite.java.tree.J.MethodInvocation;
import org.openrewrite.java.tree.J.MultiCatch;
import org.openrewrite.java.tree.J.NewArray;
import org.openrewrite.java.tree.J.NewClass;
import org.openrewrite.java.tree.J.Package;
import org.openrewrite.java.tree.J.ParameterizedType;
import org.openrewrite.java.tree.J.Parentheses;
import org.openrewrite.java.tree.J.Primitive;
import org.openrewrite.java.tree.J.Return;
import org.openrewrite.java.tree.J.Switch;
import org.openrewrite.java.tree.J.SwitchExpression;
import org.openrewrite.java.tree.J.Synchronized;
import org.openrewrite.java.tree.J.Ternary;
import org.openrewrite.java.tree.J.Throw;
import org.openrewrite.java.tree.J.Try;
import org.openrewrite.java.tree.J.Try.Catch;
import org.openrewrite.java.tree.J.Try.Resource;
import org.openrewrite.java.tree.J.TypeCast;
import org.openrewrite.java.tree.J.TypeParameter;
import org.openrewrite.java.tree.J.Unary;
import org.openrewrite.java.tree.J.VariableDeclarations;
import org.openrewrite.java.tree.J.VariableDeclarations.NamedVariable;
import org.openrewrite.java.tree.J.WhileLoop;
import org.openrewrite.java.tree.J.Wildcard;
import org.openrewrite.java.tree.J.Yield;
import org.openrewrite.java.tree.JContainer;
import org.openrewrite.java.tree.JLeftPadded;
import org.openrewrite.java.tree.JRightPadded;
import org.openrewrite.java.tree.JavaSourceFile;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.NameTree;
import org.openrewrite.java.tree.Space;
import org.openrewrite.java.tree.Space.Location;
import org.openrewrite.java.tree.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Recipe that logs each visit step, so can better understand the flow. */
public class DebuggingRecipe extends Recipe {

  private static final Logger LOG = LoggerFactory.getLogger(DebuggingRecipe.class);

  @Override
  public String getDisplayName() {
    return "Debugging recipe";
  }

  @Override
  public String getDescription() {
    return getDisplayName() + ".";
  }

  @Override
  public Set<String> getTags() {
    return ImmutableSet.of();
  }

  @Override
  public Duration getEstimatedEffortPerOccurrence() {
    return Duration.ofMinutes(2);
  }

  @Override
  public JavaVisitor<ExecutionContext> getVisitor() {
    return new Visitor();
  }

  static class Visitor extends JavaVisitor<ExecutionContext> {

    @Override
    public J visitExpression(Expression expression, ExecutionContext executionContext) {
      LOG.info("visitExpression {}", expression);
      return super.visitExpression(expression, executionContext);
    }

    @Override
    public J visitStatement(Statement statement, ExecutionContext executionContext) {
      LOG.info("statement {}", statement);
      return super.visitStatement(statement, executionContext);
    }

    @Override
    public Space visitSpace(Space space, Location loc, ExecutionContext executionContext) {
      LOG.info("space {}", space);
      return super.visitSpace(space, loc, executionContext);
    }

    @Override
    public @Nullable JavaType visitType(
        @Nullable JavaType javaType, ExecutionContext executionContext) {
      LOG.info("visitType {}", javaType);
      return super.visitType(javaType, executionContext);
    }

    @Override
    public <N extends NameTree> N visitTypeName(N nameTree, ExecutionContext executionContext) {
      LOG.info("nameTree {}", nameTree);
      return super.visitTypeName(nameTree, executionContext);
    }

    @Override
    public J visitAnnotatedType(AnnotatedType annotatedType, ExecutionContext executionContext) {
      LOG.info("annotatedType {}", annotatedType);
      return super.visitAnnotatedType(annotatedType, executionContext);
    }

    @Override
    public J visitAnnotation(Annotation annotation, ExecutionContext executionContext) {
      LOG.info("annotation {}", annotation);
      return super.visitAnnotation(annotation, executionContext);
    }

    @Override
    public J visitArrayAccess(ArrayAccess arrayAccess, ExecutionContext executionContext) {
      LOG.info("arrayAccess {}", arrayAccess);
      return super.visitArrayAccess(arrayAccess, executionContext);
    }

    @Override
    public J visitArrayDimension(ArrayDimension arrayDimension, ExecutionContext executionContext) {
      LOG.info("arrayDimension {}", arrayDimension);
      return super.visitArrayDimension(arrayDimension, executionContext);
    }

    @Override
    public J visitArrayType(ArrayType arrayType, ExecutionContext executionContext) {
      LOG.info("arrayType {}", arrayType);
      return super.visitArrayType(arrayType, executionContext);
    }

    @Override
    public J visitAssert(Assert azzert, ExecutionContext executionContext) {
      LOG.info("azzert {}", azzert);
      return super.visitAssert(azzert, executionContext);
    }

    @Override
    public J visitAssignment(Assignment assignment, ExecutionContext executionContext) {
      LOG.info("assignment {}", assignment);
      return super.visitAssignment(assignment, executionContext);
    }

    @Override
    public J visitAssignmentOperation(
        AssignmentOperation assignOp, ExecutionContext executionContext) {
      LOG.info("assignOp {}", assignOp);
      return super.visitAssignmentOperation(assignOp, executionContext);
    }

    @Override
    public J visitBinary(Binary binary, ExecutionContext executionContext) {
      LOG.info("binary {}", binary);
      return super.visitBinary(binary, executionContext);
    }

    @Override
    public J visitBlock(Block block, ExecutionContext executionContext) {
      LOG.info("block {}", block);
      return super.visitBlock(block, executionContext);
    }

    @Override
    public J visitBreak(Break breakStatement, ExecutionContext executionContext) {
      LOG.info("breakStatement {}", breakStatement);
      return super.visitBreak(breakStatement, executionContext);
    }

    @Override
    public J visitCase(Case caze, ExecutionContext executionContext) {
      LOG.info("caze {}", caze);
      return super.visitCase(caze, executionContext);
    }

    @Override
    public J visitCatch(Catch catzh, ExecutionContext executionContext) {
      LOG.info("catzh {}", catzh);
      return super.visitCatch(catzh, executionContext);
    }

    @Override
    public J visitClassDeclaration(ClassDeclaration classDecl, ExecutionContext executionContext) {
      LOG.info("classDecl {}", classDecl);
      return super.visitClassDeclaration(classDecl, executionContext);
    }

    @Override
    public J visitJavaSourceFile(JavaSourceFile cu, ExecutionContext executionContext) {
      LOG.info("visitJavaSourceFile {}", cu);
      return super.visitJavaSourceFile(cu, executionContext);
    }

    @Override
    public J visitCompilationUnit(CompilationUnit cu, ExecutionContext executionContext) {
      LOG.info("visitCompilationUnit {}", cu);
      return super.visitCompilationUnit(cu, executionContext);
    }

    @Override
    public J visitContinue(Continue continueStatement, ExecutionContext executionContext) {
      LOG.info("continueStatement {}", continueStatement);
      return super.visitContinue(continueStatement, executionContext);
    }

    @Override
    public <T extends J> J visitControlParentheses(
        ControlParentheses<T> controlParens, ExecutionContext executionContext) {
      LOG.info("controlParens {}", controlParens);
      return super.visitControlParentheses(controlParens, executionContext);
    }

    @Override
    public J visitDoWhileLoop(DoWhileLoop doWhileLoop, ExecutionContext executionContext) {
      LOG.info("doWhileLoop {}", doWhileLoop);
      return super.visitDoWhileLoop(doWhileLoop, executionContext);
    }

    @Override
    public J visitEmpty(Empty empty, ExecutionContext executionContext) {
      LOG.info("empty {}", empty);
      return super.visitEmpty(empty, executionContext);
    }

    @Override
    public J visitEnumValue(EnumValue enoom, ExecutionContext executionContext) {
      LOG.info("enoom {}", enoom);
      return super.visitEnumValue(enoom, executionContext);
    }

    @Override
    public J visitEnumValueSet(EnumValueSet enums, ExecutionContext executionContext) {
      LOG.info("enums {}", enums);
      return super.visitEnumValueSet(enums, executionContext);
    }

    @Override
    public J visitFieldAccess(FieldAccess fieldAccess, ExecutionContext executionContext) {
      LOG.info("fieldAccess {}", fieldAccess);
      return super.visitFieldAccess(fieldAccess, executionContext);
    }

    @Override
    public J visitForEachLoop(ForEachLoop forLoop, ExecutionContext executionContext) {
      LOG.info("forLoop {}", forLoop);
      return super.visitForEachLoop(forLoop, executionContext);
    }

    @Override
    public J visitForEachControl(Control control, ExecutionContext executionContext) {
      LOG.info("visitForEachControl {}", control);
      return super.visitForEachControl(control, executionContext);
    }

    @Override
    public J visitForLoop(ForLoop forLoop, ExecutionContext executionContext) {
      LOG.info("forLoop {}", forLoop);
      return super.visitForLoop(forLoop, executionContext);
    }

    @Override
    public J visitForControl(ForLoop.Control control, ExecutionContext executionContext) {
      LOG.info("visitForControl {}", control);
      return super.visitForControl(control, executionContext);
    }

    @Override
    public J visitIdentifier(J.Identifier ident, ExecutionContext executionContext) {
      LOG.info("ident {}", ident);
      return super.visitIdentifier(ident, executionContext);
    }

    @Override
    public J visitElse(Else elze, ExecutionContext executionContext) {
      LOG.info("elze {}", elze);
      return super.visitElse(elze, executionContext);
    }

    @Override
    public J visitIf(If iff, ExecutionContext executionContext) {
      LOG.info("iff {}", iff);
      return super.visitIf(iff, executionContext);
    }

    @Override
    public J visitImport(Import impoort, ExecutionContext executionContext) {
      LOG.info("impoort {}", impoort);
      return super.visitImport(impoort, executionContext);
    }

    @Override
    public J visitInstanceOf(InstanceOf instanceOf, ExecutionContext executionContext) {
      LOG.info("instanceOf {}", instanceOf);
      return super.visitInstanceOf(instanceOf, executionContext);
    }

    @Override
    public J visitLabel(Label label, ExecutionContext executionContext) {
      LOG.info("label {}", label);
      return super.visitLabel(label, executionContext);
    }

    @Override
    public J visitLambda(Lambda lambda, ExecutionContext executionContext) {
      LOG.info("lambda {}", lambda);
      return super.visitLambda(lambda, executionContext);
    }

    @Override
    public J visitLiteral(Literal literal, ExecutionContext executionContext) {
      LOG.info("literal {}", literal);
      return super.visitLiteral(literal, executionContext);
    }

    @Override
    public J visitMemberReference(MemberReference memberRef, ExecutionContext executionContext) {
      LOG.info("memberRef {}", memberRef);
      return super.visitMemberReference(memberRef, executionContext);
    }

    @Override
    public J visitMethodDeclaration(MethodDeclaration method, ExecutionContext executionContext) {
      LOG.info("method {}", method);
      return super.visitMethodDeclaration(method, executionContext);
    }

    @Override
    public J visitMethodInvocation(MethodInvocation method, ExecutionContext executionContext) {
      LOG.info("method {}", method);
      return super.visitMethodInvocation(method, executionContext);
    }

    @Override
    public J visitMultiCatch(MultiCatch multiCatch, ExecutionContext executionContext) {
      LOG.info("multiCatch {}", multiCatch);
      return super.visitMultiCatch(multiCatch, executionContext);
    }

    @Override
    public J visitVariableDeclarations(
        VariableDeclarations multiVariable, ExecutionContext executionContext) {
      LOG.info("multiVariable {}", multiVariable);
      return super.visitVariableDeclarations(multiVariable, executionContext);
    }

    @Override
    public J visitNewArray(NewArray newArray, ExecutionContext executionContext) {
      LOG.info("newArray {}", newArray);
      return super.visitNewArray(newArray, executionContext);
    }

    @Override
    public J visitNewClass(NewClass newClass, ExecutionContext executionContext) {
      LOG.info("newClass {}", newClass);
      return super.visitNewClass(newClass, executionContext);
    }

    @Override
    public J visitPackage(Package pkg, ExecutionContext executionContext) {
      LOG.info("pkg {}", pkg);
      return super.visitPackage(pkg, executionContext);
    }

    @Override
    public J visitParameterizedType(ParameterizedType type, ExecutionContext executionContext) {
      LOG.info("type {}", type);
      return super.visitParameterizedType(type, executionContext);
    }

    @Override
    public <T extends J> J visitParentheses(
        Parentheses<T> parens, ExecutionContext executionContext) {
      LOG.info("parens {}", parens);
      return super.visitParentheses(parens, executionContext);
    }

    @Override
    public J visitPrimitive(Primitive primitive, ExecutionContext executionContext) {
      LOG.info("primitive {}", primitive);
      return super.visitPrimitive(primitive, executionContext);
    }

    @Override
    public J visitReturn(Return retrn, ExecutionContext executionContext) {
      LOG.info("retrn {}", retrn);
      return super.visitReturn(retrn, executionContext);
    }

    @Override
    public J visitSwitch(Switch switzh, ExecutionContext executionContext) {
      LOG.info("visitSwitch {}", switzh);
      return super.visitSwitch(switzh, executionContext);
    }

    @Override
    public J visitSwitchExpression(SwitchExpression switzh, ExecutionContext executionContext) {
      LOG.info("visitSwitchExpression {}", switzh);
      return super.visitSwitchExpression(switzh, executionContext);
    }

    @Override
    public J visitSynchronized(Synchronized synch, ExecutionContext executionContext) {
      LOG.info("synch {}", synch);
      return super.visitSynchronized(synch, executionContext);
    }

    @Override
    public J visitTernary(Ternary ternary, ExecutionContext executionContext) {
      LOG.info("ternary {}", ternary);
      return super.visitTernary(ternary, executionContext);
    }

    @Override
    public J visitThrow(Throw thrown, ExecutionContext executionContext) {
      LOG.info("thrown {}", thrown);
      return super.visitThrow(thrown, executionContext);
    }

    @Override
    public J visitTry(Try tryable, ExecutionContext executionContext) {
      LOG.info("tryable {}", tryable);
      return super.visitTry(tryable, executionContext);
    }

    @Override
    public J visitTryResource(Resource tryResource, ExecutionContext executionContext) {
      LOG.info("tryResource {}", tryResource);
      return super.visitTryResource(tryResource, executionContext);
    }

    @Override
    public J visitTypeCast(TypeCast typeCast, ExecutionContext executionContext) {
      LOG.info("typeCast {}", typeCast);
      return super.visitTypeCast(typeCast, executionContext);
    }

    @Override
    public J visitTypeParameter(TypeParameter typeParam, ExecutionContext executionContext) {
      LOG.info("typeParam {}", typeParam);
      return super.visitTypeParameter(typeParam, executionContext);
    }

    @Override
    public J visitUnary(Unary unary, ExecutionContext executionContext) {
      LOG.info("unary {}", unary);
      return super.visitUnary(unary, executionContext);
    }

    @Override
    public J visitVariable(NamedVariable variable, ExecutionContext executionContext) {
      LOG.info("variable {}", variable);
      return super.visitVariable(variable, executionContext);
    }

    @Override
    public J visitWhileLoop(WhileLoop whileLoop, ExecutionContext executionContext) {
      LOG.info("whileLoop {}", whileLoop);
      return super.visitWhileLoop(whileLoop, executionContext);
    }

    @Override
    public J visitWildcard(Wildcard wildcard, ExecutionContext executionContext) {
      LOG.info("wildcard {}", wildcard);
      return super.visitWildcard(wildcard, executionContext);
    }

    @Override
    public J visitYield(Yield yield, ExecutionContext executionContext) {
      LOG.info("yield {}", yield);
      return super.visitYield(yield, executionContext);
    }

    @Override
    public <T> JRightPadded<T> visitRightPadded(
        @Nullable JRightPadded<T> right,
        JRightPadded.Location loc,
        ExecutionContext executionContext) {
      LOG.info("right {}", right);
      return super.visitRightPadded(right, loc, executionContext);
    }

    @Override
    public <T> JLeftPadded<T> visitLeftPadded(
        @Nullable JLeftPadded<T> left,
        JLeftPadded.Location loc,
        ExecutionContext executionContext) {
      LOG.info("left {}", left);
      return super.visitLeftPadded(left, loc, executionContext);
    }

    @Override
    public <J2 extends J> JContainer<J2> visitContainer(
        @Nullable JContainer<J2> container,
        JContainer.Location loc,
        ExecutionContext executionContext) {
      LOG.info("container {}", container);
      return super.visitContainer(container, loc, executionContext);
    }
  }
}
