package edu.buffalo.cse562;

import java.io.File;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;

public class SelectEvaluator implements SelectVisitor {
	public IOperator operator = null;
	File dataDir = null;
	HashMap<String, CreateTable> tables = new HashMap<String, CreateTable>();
	// public static List<String> inList=new ArrayList<String>();
	public static boolean orderByFlag;
	public static boolean aggrFlag;
	public static boolean forPushdownProject = false;

	StatementEvaluator evaluator = new StatementEvaluator();

	public SelectEvaluator(IOperator operator, File dataDir,
			HashMap<String, CreateTable> tables) {

		this.operator = operator;
		this.dataDir = dataDir;
		this.tables = tables;
		// TODO Auto-generated constructor stub
	}

	private HashMap<String, Expression> getTableAndJoinExpression(
			PlainSelect pselect, ArrayList<String> tablesBeingJoined,
			ArrayList<String> tablesNotBeingJoined,
			ArrayList<String> conditions, boolean isFirstJoin) {
		ArrayList<String> evaluatedConditions = new ArrayList<String>(
				conditions);

		HashMap<String, Expression> exprMap = new HashMap<String, Expression>();
		StringBuilder leftExpr = new StringBuilder();
		StringBuilder rightExpr = new StringBuilder();
		StringBuilder joinExpr = new StringBuilder();

		List<?> joins = pselect.getJoins();

		for (int i = 0; i < joins.size(); i++) {

			String tabName = joins.get(i).toString();

			if (tabName.contains(" AS ")) {
				Matcher matcher = Pattern.compile("(?<= AS ).*").matcher(
						tabName);
				matcher.find();
				tabName = matcher.group();
			}
			if (!(tablesBeingJoined.contains(tabName))) {

				tablesNotBeingJoined.add(tabName);
			}
		}

		for (int i = 0; i < tablesNotBeingJoined.size(); i++) {
			for (int j = 0; j < evaluatedConditions.size(); j++) {
				if (evaluatedConditions.get(j).contains(
						tablesNotBeingJoined.get(i) + ".")) {
					evaluatedConditions.remove(j);
					--j;

				}
			}
		}

		for (int i = 0; i < evaluatedConditions.size(); i++) {

			String condition = evaluatedConditions.get(i);

			// TODO: extend for "OR"
			if (isFirstJoin
					&& condition.contains(tablesBeingJoined.get(0) + ".")
					&& !(condition.contains(tablesBeingJoined.get(1) + "."))) {
				if (leftExpr.length() != 0) {
					leftExpr.append("AND");
				}
				leftExpr.append(condition);

			} else if (condition.contains(tablesBeingJoined
					.get(tablesBeingJoined.size() - 1) + ".")) {
				boolean isJoinCondition = false;
				// check if the right table joins with any of the tables seen
				// till now and set the flag if true
				for (int k = 0; k < tablesBeingJoined.size() - 1; k++) {
					if (condition.contains(tablesBeingJoined.get(k) + ".")) {
						isJoinCondition = true;
					}
				}

				if (!isJoinCondition) {
					// condition contains right table and none of the left
					// tables
					if (rightExpr.length() != 0) {
						rightExpr.append("AND");
					}
					rightExpr.append(condition);
				}

				else {
					if (joinExpr.length() != 0) {
						joinExpr.append("AND");
					}
					joinExpr.append(condition);
				}
			}
		}

		exprMap = getExpressionList(leftExpr.toString(), rightExpr.toString(),
				joinExpr.toString());

		for (int k = 0; k < conditions.size(); k++) {
			if (evaluatedConditions.contains(conditions.get(k))) {
				conditions.remove(k);
				--k;
			}
		}

		return exprMap;
	}

	private HashMap<String, Expression> getExpressionList(String leftExpr,
			String rightExpr, String joinExpr) {
		HashMap<String, Expression> exprMap = new HashMap<String, Expression>();
		Expression lExpr = null;
		Expression rExpr = null;
		Expression jExpr = null;

		try {
			StringReader strReader = null;
			CCJSqlParser parser = null;
			if (joinExpr.length() != 0) {
				strReader = new StringReader(joinExpr);
				parser = new CCJSqlParser(strReader);
				jExpr = parser.Expression();
				exprMap.put("join", jExpr);
			}
			if (rightExpr.length() != 0) {
				strReader = new StringReader(rightExpr);
				parser = new CCJSqlParser(strReader);
				rExpr = parser.Expression();
				exprMap.put("right", rExpr);
			}
			if (leftExpr.length() != 0) {
				strReader = new StringReader(leftExpr);
				parser = new CCJSqlParser(strReader);
				lExpr = parser.Expression();
				exprMap.put("left", lExpr);
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return exprMap;
	}

	private ArrayList<String> splitConditions(String whereExpr) {

		ArrayList<String> whereConditions = new ArrayList<String>();
		ArrayList<String> orConditions = new ArrayList<String>();
		ArrayList<String> priorityConditions = new ArrayList<String>();
		ArrayList<String> notInConditions = new ArrayList<String>();
		Scanner sc = new Scanner(whereExpr);
		sc.useDelimiter("AND");

		while (sc.hasNext()) {
			String condition = sc.next();

			if (condition.contains(" OR ")) {
				orConditions.add(condition);
			} else if (condition.contains(" = ")) {
				priorityConditions.add(condition);
			} else if (condition.contains(" IN ")) {
				orConditions.add(condition);
				//populateInMap(condition);
			}else if(condition.contains(" NOT IN ")) {
				notInConditions.add(condition);
			}
			else {
				whereConditions.add(condition);
			}
		}
		sc.close();
		whereConditions.addAll(priorityConditions);
		orConditions.addAll(notInConditions);
		orConditions.addAll(whereConditions);
		return orConditions;
	}


	private HashMap<String, ArrayList<SelectItem>> pushDownProjection(
			PlainSelect pselect, ArrayList<String> tablesBeingJoined,
			ArrayList<String> tablesNotBeingJoined,
			StatementEvaluator evaluator, boolean isFirstJoin)
			throws ParseException {

		HashMap<String, ArrayList<SelectItem>> colsMap = new HashMap<String, ArrayList<SelectItem>>();
		ArrayList<SelectItem> leftCols = new ArrayList<SelectItem>();
		ArrayList<SelectItem> rightCols = new ArrayList<SelectItem>();

		ArrayList<String> evaluatedCols = new ArrayList<String>(
				evaluator.CompListofColNames);

		for (int i = 0; i < tablesNotBeingJoined.size(); i++) {
			for (int j = 0; j < evaluatedCols.size(); j++) {
				if (evaluatedCols.get(j).toString()
						.contains(tablesNotBeingJoined.get(i) + ".")) {
					evaluatedCols.remove(j);
					--j;
				}
			}
		}

		StringReader strReader = null;
		CCJSqlParser parser = null;

		for (int i = 0; i < evaluatedCols.size(); i++) {

			String col = evaluatedCols.get(i);
			strReader = new StringReader(col);
			parser = new CCJSqlParser(strReader);
			if (isFirstJoin && col.contains(tablesBeingJoined.get(0) + ".")) {

				leftCols.add(parser.SelectItem());

			} else {
				rightCols.add(parser.SelectItem());
			}
		}

		if (leftCols.size() != 0) {

			colsMap.put("left", leftCols);
		}
		if (rightCols.size() != 0) {
			colsMap.put("right", rightCols);
		}

		for (int k = 0; k < evaluator.CompListofColNames.size(); k++) {
			if (evaluatedCols.contains(evaluator.CompListofColNames.get(k))) {
				evaluator.CompListofColNames.remove(k);
				--k;
			}
		}
		StatementEvaluator.isProjectCheck = false;
		return colsMap;

	}

	private HashMap<String, ArrayList<String>> populateReqColsMap(
			PlainSelect pselect, StatementEvaluator evaluator,
			ArrayList<String> whereconditions) {

		List<Expression> Expressions = pselect.getSelectItems();
		HashMap<String, ArrayList<String>> reqColsMap = new HashMap<String, ArrayList<String>>();
		evaluator.isProjectCheck = true;
		evaluator.isSelGrpByOrderBy = true;
		// adding columns in select expression
		for (int j = 0; j < Expressions.size(); j++) {
			SelectExpressionItem sEItem = (SelectExpressionItem) Expressions
					.get(j);
			Expression expr = sEItem.getExpression();

			expr.accept(evaluator);

		}

		// adding columns in group by
		if (pselect.getGroupByColumnReferences() != null) {
			List<Expression> grpByCols = pselect.getGroupByColumnReferences();
			for (int k = 0; k < grpByCols.size(); k++) {
				Column expr = (Column) grpByCols.get(k);
				String str = expr.toString();
				// making sure that aliases are eliminated
				if (expr.toString().contains(".")) {
					expr.accept(evaluator);
				}
			}
		}

		// adding columns in order by
		if (pselect.getOrderByElements() != null) {
			List<OrderByElement> orderByCols = pselect.getOrderByElements();
			for (int k = 0; k < orderByCols.size(); k++) {

				Column expr = (Column) orderByCols.get(k).getExpression();
				// making sure that aliases are eliminated
				if ((expr.toString().contains("."))) {
					expr.accept(evaluator);

				}
			}

		}
		evaluator.isSelGrpByOrderBy = false;

		// add columns in where conditions

		StringReader strReader = null;

		CCJSqlParser parser = null;
		for (int j = 0; j < whereconditions.size(); j++) {
			String str = whereconditions.get(j);
			strReader = new StringReader(str);
			parser = new CCJSqlParser(strReader);
			try {
				Expression expr = parser.Expression();

				if (expr instanceof Parenthesis) {
					evaluator.isNotJoinCol = true;
				} else {
					BinaryExpression bexpr = (BinaryExpression) expr;
					Expression lExpr = bexpr.getLeftExpression();
					Expression rExpr = bexpr.getRightExpression();
					String lTabName = null;
					String rTabName = null;

					if (!(lExpr instanceof Column && rExpr instanceof Column)) {
						evaluator.isNotJoinCol = true;
					} else {
						Matcher matcher = null;
						matcher = Pattern.compile(".*(?=\\.)").matcher(
								lExpr.toString());
						matcher.find();
						lTabName = matcher.group();
						matcher = Pattern.compile(".*(?=\\.)").matcher(
								rExpr.toString());
						matcher.find();
						rTabName = matcher.group();
						if (lTabName.equals(rTabName)) {

							evaluator.isNotJoinCol = true;

						}
					}
				}
				expr.accept(evaluator);
				evaluator.isNotJoinCol = false;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		reqColsMap = populateReqColsMapHelper(evaluator.CompListofColNames);
		StatementEvaluator.isProjectCheck = false;
		return reqColsMap;

	}

	private void removeWhereColumns(StatementEvaluator evaluator) {
		for (int i = 0; i < evaluator.CompListofColNames.size(); i++) {
			String colName = evaluator.CompListofColNames.get(i);
			if ((evaluator.notJoinCols.contains(colName))
					&& !(evaluator.selGrpbyOrderbyCols.contains(colName))) {
				evaluator.CompListofColNames.remove(i);
				--i;
			}
		}

	}

	private HashMap<String, ArrayList<String>> populateReqColsMapHelper(
			ArrayList<String> compListofCols) {
		HashMap<String, ArrayList<String>> reqColsMap = new HashMap<String, ArrayList<String>>();
		ArrayList<String> cols = null;

		for (int j = 0; j < compListofCols.size(); j++) {
			cols = new ArrayList<String>();
			String colName = compListofCols.get(j);
			String tabNameForCol = null;
			Matcher matcher = Pattern.compile(".*(?=\\.)").matcher(colName);
			boolean status = matcher.find();
			if (status) {
				tabNameForCol = matcher.group();
				if (!reqColsMap.containsKey(tabNameForCol)) {
					cols.add(colName);
					reqColsMap.put(tabNameForCol, cols);
				} else {
					cols = reqColsMap.get(tabNameForCol);
					cols.add(colName);
					reqColsMap.put(tabNameForCol, cols);
				}
			}
		}

		return reqColsMap;

	}

	@Override
	public void visit(PlainSelect pselect) {
		Column[] schema = null;
		ArrayList<String> whereconditions = new ArrayList<String>();
		ArrayList<String> tablesBeingJoined = new ArrayList<String>();
		HashMap<String, ArrayList<String>> requiredColsInTables = new HashMap<String, ArrayList<String>>();

		String leftTableName = null;
		StatementEvaluator evaluator = new StatementEvaluator();

		Expression whereClause = pselect.getWhere();
		if (whereClause != null) {
			whereconditions = splitConditions(whereClause.toString());
			requiredColsInTables = populateReqColsMap(pselect, evaluator,
					whereconditions);
			// remove all columns in where other than join columns
			removeWhereColumns(evaluator);
		}

		FromScanner fromScan = null;

		if (!requiredColsInTables.isEmpty()) {
			fromScan = new FromScanner(dataDir, tables, requiredColsInTables);
			pselect.getFromItem().accept(fromScan);
		} else {
			fromScan = new FromScanner(dataDir, tables);
			pselect.getFromItem().accept(fromScan);
		}

		operator = fromScan.source;

		boolean isFirstJoin = false;
		Column[] leftschema = operator.getSchema();

		if (leftschema[0].getTable().getAlias() != null) {
			leftTableName = leftschema[0].getTable().getAlias();
		} else {
			leftTableName = leftschema[0].getTable().getName();
		}
		tablesBeingJoined.add(leftTableName);
		

		if (pselect.getJoins() != null) {
			
			int numjoins = pselect.getJoins().size();
			ArrayList<Join> joinTables = new ArrayList<Join>();
			List<?> joins = pselect.getJoins();

			// generate a custom joins table so that you can add/remove items as
			// required
			for (int i = 0; i < joins.size(); i++) {
				Join aJoin = (Join) joins.get(i);
				joinTables.add(aJoin);
			}

			for (int i = 0; i < joinTables.size(); i++) {
				ArrayList<String> tablesNotBeingJoined = new ArrayList<String>();
				HashMap<String, Expression> exprMap = new HashMap<String, Expression>();
				HashMap<String, ArrayList<SelectItem>> colsMap = new HashMap<String, ArrayList<SelectItem>>();
				String rightTableName = null;
				ArrayList<String> whereconditions_bk = new ArrayList<String>(
						whereconditions);

				if (i == 0) {
					isFirstJoin = true;
				}

				Join rightjoin = (Join) joinTables.get(i);

				if (rightjoin.toString().contains(" AS ")) {
					// to retrieve alias from table being joined
					// Eg: Orders AS o1 => retrieves o1
					Matcher matcher = Pattern.compile("(?<= AS ).*").matcher(
							rightjoin.toString());
					matcher.find();
					rightTableName = matcher.group();
				} else {
					rightTableName = rightjoin.toString();
				}

				tablesBeingJoined.add(rightTableName);
				exprMap = getTableAndJoinExpression(pselect, tablesBeingJoined,
						tablesNotBeingJoined, whereconditions, isFirstJoin);

				if (exprMap.containsKey("join")) {

					try {
						colsMap = pushDownProjection(pselect,
								tablesBeingJoined, tablesNotBeingJoined,
								evaluator, isFirstJoin);
					} catch (ParseException e) { // TODO Auto-generated catch
													// block
						e.printStackTrace();
					}
				}

				// pushing down selections
				// pass left table conditions
				if (isFirstJoin && exprMap.containsKey("left")
						&& exprMap.containsKey("join")) {
					operator = new selectOperator(operator,
							operator.getSchema(), exprMap.get("left"));
				}

				if (isFirstJoin && exprMap.containsKey("join")
						&& colsMap.containsKey("left")) {
					// send list of columns (of the left table) you want to //
					// retain instead of select items
					if (colsMap.get("left").size() < operator.getSchema().length) {
						forPushdownProject = true;
						operator = new projectOperator(operator,
								operator.getSchema(), colsMap.get("left"));
						forPushdownProject = false;

					}
				}

				IOperator rightJoinOperator = null;
				if (exprMap.containsKey("join")) {
					rightjoin.getRightItem().accept(fromScan);
					rightJoinOperator = fromScan.source;
					schema = rightJoinOperator.getSchema();
				}
				// pass right table conditions
				if (exprMap.containsKey("right") && exprMap.containsKey("join")) {
					rightJoinOperator = new selectOperator(rightJoinOperator,
							rightJoinOperator.getSchema(), exprMap.get("right"));
				}

				if (exprMap.containsKey("join") && colsMap.containsKey("right")) {
					// send list of columns (of the right table) you want to //
					// retain instead of select items
					if (colsMap.get("right").size() < rightJoinOperator
							.getSchema().length) {
						forPushdownProject = true;
						rightJoinOperator = new projectOperator(
								rightJoinOperator,
								rightJoinOperator.getSchema(),
								colsMap.get("right"));
						schema = rightJoinOperator.getSchema();
						forPushdownProject = false;
					}
				}
				
				// pass only join conditions
				if (exprMap.containsKey("join")) {
					
					if(numjoins==1)
						operator = new SortMergeJoinOperator(operator,
								rightJoinOperator, exprMap.get("join"), schema);
					else if (Main.swapFlag == true)
						operator = new HybridHJoinOperator_Hash(operator,
								rightJoinOperator, exprMap.get("join"), schema);
					else
						operator = new JoinOperator(operator,
								rightJoinOperator, exprMap.get("join"), schema);
				} else {
					// In the case that tables being joined are not adjacent to
					// each other in from clause (with no join condition between
					// them)
					// undo changes to tablesBeingChanged and whereconditions
					tablesBeingJoined.remove(rightTableName);
					whereconditions = whereconditions_bk;
					Join tempJoin = joinTables.get(i);
					// remove table from custom list and add to the tail of the
					// list to be processed later
					joinTables.remove(i);
					joinTables.add(tempJoin);
					--i;
				}
			}

		}

		if (pselect.getWhere() != null && pselect.getJoins() == null) {
			operator = new selectOperator(operator, operator.getSchema(),
					pselect.getWhere());
		}

		for (int j = 0; j < pselect.getSelectItems().size(); j++) {
			SelectExpressionItem sEItem = (SelectExpressionItem) pselect
					.getSelectItems().get(j);
			Expression expr = sEItem.getExpression();

			// expr.accept(evaluator);
			if (expr instanceof Function) {
				evaluator.AggFuncFlag_tmp = true;
				break;
			}
		}

		// GROUP BY and AGGREGATES
		if (evaluator.AggFuncFlag_tmp == true) {
			aggrFlag = true;
			operator = new AggregateOperator(operator, operator.getSchema(),
					pselect);
		}

		// ORDER BY clause
		if ((pselect.getOrderByElements() != null)) {
			orderByFlag = true;
			operator = new OrderByOperator(operator, operator.getSchema(),
					pselect);
		}
		// end of ORDER BY evaluation

		// Go to project only if Aggregate operator is not present, otherwise
		// aggregate operator handles everything
		if (pselect.getSelectItems() != null
				&& evaluator.AggFuncFlag_tmp == false) {
			operator = new projectOperator(operator, operator.getSchema(),
					pselect.getSelectItems());
		}
	}

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub

	}
}
