// Generated from /Users/shuzheng/IdeaProjects/bigdata-scala/src/main/scala/com/lisz/bigdata/sql/g4/aaa.g4 by ANTLR 4.9.1
package com.lisz.bigdata.sql.parser;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

/**
 * This class provides an empty implementation of {@link aaaVisitor},
 * which can be extended to create a visitor which only needs to handle a subset
 * of the available methods.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public class aaaBaseVisitor<T> extends AbstractParseTreeVisitor<T> implements aaaVisitor<T> {
	/**
	 * {@inheritDoc}
	 *
	 * <p>The default implementation returns the result of calling
	 * {@link #visitChildren} on {@code ctx}.</p>
	 */
	@Override public T visitAaainit(aaaParser.AaainitContext ctx) { return visitChildren(ctx); }
	/**
	 * {@inheritDoc}
	 *
	 * <p>The default implementation returns the result of calling
	 * {@link #visitChildren} on {@code ctx}.</p>
	 */
	@Override public T visitValue(aaaParser.ValueContext ctx) { return visitChildren(ctx); }
}