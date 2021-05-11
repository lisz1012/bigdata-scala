// Generated from /Users/shuzheng/IdeaProjects/bigdata-scala/src/main/scala/com/lisz/bigdata/sql/g4/aaa.g4 by ANTLR 4.9.1
package com.lisz.bigdata.sql.parser;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link aaaParser}.
 */
public interface aaaListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link aaaParser#aaainit}.
	 * @param ctx the parse tree
	 */
	void enterAaainit(aaaParser.AaainitContext ctx);
	/**
	 * Exit a parse tree produced by {@link aaaParser#aaainit}.
	 * @param ctx the parse tree
	 */
	void exitAaainit(aaaParser.AaainitContext ctx);
	/**
	 * Enter a parse tree produced by {@link aaaParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(aaaParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link aaaParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(aaaParser.ValueContext ctx);
}