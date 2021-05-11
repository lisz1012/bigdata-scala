// Generated from /Users/shuzheng/IdeaProjects/bigdata-scala/src/main/scala/com/lisz/bigdata/sql/g4/aaa.g4 by ANTLR 4.9.1
package com.lisz.bigdata.sql.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link aaaParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface aaaVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link aaaParser#aaainit}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitAaainit(aaaParser.AaainitContext ctx);
	/**
	 * Visit a parse tree produced by {@link aaaParser#value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue(aaaParser.ValueContext ctx);
}