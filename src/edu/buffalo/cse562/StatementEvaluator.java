package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.*;

public class StatementEvaluator extends AbstractExpressionAndStatementVisitor {

	private boolean resultFlag;
	public Column[] schema;
	public IDatum[] colValues;
	private IDatum colValue;
	public String tableName;
	private String type;
	public Function aggregateFn;
	public int columnIndex = 0;
	private static boolean AggFuncFlag = false;
	public boolean AggFuncFlag_tmp = false;
	public String exprName = null;
	public Column AggOnColumn = null;
	public static boolean isProjectCheck = false;
	public static boolean isNotJoinCol = false;
	public static boolean isSelGrpByOrderBy = false;

	public HashMap<String, IDatum> myColumnDetails = new HashMap<String, IDatum>();
	public HashMap<String, Integer> myColumnIndexes = new HashMap<String, Integer>();
	public HashMap<String, IDatum> wasMyColumnDetails = new HashMap<String, IDatum>();
	public HashMap<String, String> aliasToTable = new HashMap<String, String>();
	public HashMap<String, String> columnToTable = new HashMap<String, String>();
	public ArrayList<String> CompListofColNames = new ArrayList<String>();
	public ArrayList<String> notJoinCols = new ArrayList<String>();
	public ArrayList<String> selGrpbyOrderbyCols = new ArrayList<String>();

	public StatementEvaluator(Column[] schema, IDatum[] colValues) {
		this.schema = schema;
		this.colValues = colValues;
		this.aggregateFn = null;
		for (int i = 0; i < colValues.length; i++) {
			Column col = schema[i];
			Table tab = col.getTable();
			myColumnDetails.put(tab.getName() + "|" + col.getColumnName(),
					colValues[i]);
			wasMyColumnDetails.put(col.getColumnName(), colValues[i]);
			aliasToTable.put(tab.getAlias(), tab.getName());
			columnToTable.put(col.getColumnName(), tab.getName());
		}
	}
	
	public StatementEvaluator(Column[] schema, IDatum[] colValues, String[] colNames, String[] colwithTblNames, 
			 HashMap<String,String> aliasToTable, HashMap<String,String> columnToTable) {
		this.schema = schema;
		this.colValues = colValues;
		this.aggregateFn = null;
		this.aliasToTable = aliasToTable;
		this.columnToTable = columnToTable;
		
		for (int i = 0; i < colValues.length; i++) {
			myColumnDetails.put(colwithTblNames[i],colValues[i]);
			wasMyColumnDetails.put(colNames[i], colValues[i]);
		}
	}

	public StatementEvaluator() {
		this.schema = null;
		this.colValues = null;
		this.aggregateFn = null;
		myColumnDetails = null;
	}

	public StatementEvaluator(Column[] schema) {
		this.schema = schema;
		this.colValues = null;
		this.aggregateFn = null;
		myColumnDetails = null;
		for (int i = 0; i < schema.length; i++) {
			Column col=schema[i];
			Table tab=col.getTable();	
			aliasToTable.put(tab.getAlias(), tab.getName());
			columnToTable.put(col.getColumnName(), tab.getName());
		}
	}
	
	public StatementEvaluator(Column[] schema,Column col) {
		String table_or_alias = null;
		String column_key = null;
		StringBuilder column_key_tmp = new StringBuilder();
		this.schema = schema;
		this.colValues = null;
		this.aggregateFn = null;
		myColumnDetails = null;
		for (int i = 0; i < schema.length; i++) {
			Column col_tmp=schema[i];
			Table tab=col_tmp.getTable();
			myColumnIndexes.put(tab.getName() + "|" + col_tmp.getColumnName(),i);
			aliasToTable.put(tab.getAlias(), tab.getName());
			columnToTable.put(col_tmp.getColumnName(), tab.getName());
		}
		
		table_or_alias = col.getTable().getName();

		if (table_or_alias == null) {
			column_key_tmp.append(columnToTable.get(col.getColumnName()));
		} else if (aliasToTable.get(table_or_alias) != null) {
			column_key_tmp.append(aliasToTable.get(table_or_alias));
		} else {
			column_key_tmp.append(table_or_alias);
		}

		column_key_tmp.append("|");
		column_key_tmp.append(col.getColumnName());
		column_key = column_key_tmp.toString();
		columnIndex = myColumnIndexes.get(column_key);
	}
	public boolean getResult() {
		return resultFlag;
	}

	public static boolean getAggFuncFlag() {
		return AggFuncFlag;
	}

	public IDatum getColValue() {
		return colValue;
	}

	public String getType() {
		return type;
	}

	@Override
	public void visit(LongValue lv) {
		if (!isProjectCheck) {
			colValue = new integerDatum((String.valueOf(lv.getValue())));
		}
	}

	@Override
	public void visit(LikeExpression arg) {
		LikeExpression expr = (LikeExpression) arg;
		expr.getLeftExpression().accept(this);
		String leftValue = (String) colValue.getValue();

		expr.getRightExpression().accept(this);
		String rightValue = (String) colValue.getValue();
		if (rightValue.contains("%")) {
			rightValue = rightValue.replaceAll(Pattern.quote("%"), ".*");
		}
		if (rightValue.contains("_")) {
			rightValue = rightValue.replaceAll(Pattern.quote("_"), ".");
		}

		resultFlag = Pattern.matches(rightValue, leftValue);

	}

	@Override
	public void visit(Column col) {
		String table_or_alias = null;
		String column_key = null;
		StringBuilder column_key_tmp = new StringBuilder();
		String table_name = null;

		if (isProjectCheck) {
			if (isNotJoinCol) {
				if (!notJoinCols.contains(col.getWholeColumnName())) {
					notJoinCols.add(col.getWholeColumnName());
				}
			}
			if (isSelGrpByOrderBy) {
				if (!selGrpbyOrderbyCols.contains(col.getWholeColumnName())) {
					selGrpbyOrderbyCols.add(col.getWholeColumnName());
				}
			}
			if (!CompListofColNames.contains(col.getWholeColumnName())) {
				CompListofColNames.add(col.getWholeColumnName());
			}
		}
		else{
		exprName = col.getColumnName();

		table_or_alias = col.getTable().getName();

		if (table_or_alias == null) {
			column_key_tmp.append(columnToTable.get(col.getColumnName()));
			column_key_tmp.append("|");
			column_key_tmp.append(col.getColumnName());
			column_key = column_key_tmp.toString();
			table_name = columnToTable.get(col.getColumnName());
		} else if (aliasToTable.get(table_or_alias) != null) {
			column_key_tmp.append(aliasToTable.get(table_or_alias));
			column_key_tmp.append("|");
			column_key_tmp.append(col.getColumnName());
			column_key = column_key_tmp.toString();
			table_name = aliasToTable.get(table_or_alias);
		} else {
			column_key_tmp.append(table_or_alias);
			column_key_tmp.append("|");
			column_key_tmp.append(col.getColumnName());
			column_key = column_key_tmp.toString();
			table_name = table_or_alias;
		}
		if (myColumnDetails != null) {
			IDatum exprValue = myColumnDetails.get(column_key);
			colValue = exprValue;
		}
		}

		tableName = table_name;
		type = FromScanner.colDetails.get(exprName);

	}

	@Override
	public void visit(Function func) {
		Function fn = (Function) func;
		aggregateFn = fn;
		if (!(fn.getName().toUpperCase().equals("DATE"))) {
			AggFuncFlag = true;
			AggFuncFlag_tmp = true;
		}

		if (isProjectCheck) {
			ExpressionList paramList1 = fn.getParameters();
			if (paramList1 != null) {
				List<?> Expressions = paramList1.getExpressions();
				for (int i = 0; i < Expressions.size(); i++) {
					Expression expr = (Expression) Expressions.get(i);
					expr.accept(this);
				}

			}
		}

		if (myColumnDetails != null) {
			ExpressionList paramList = fn.getParameters();
			if (paramList != null) {
				List<?> Expressions = paramList.getExpressions();

				if (Expressions.get(0) instanceof Column) {
					AggOnColumn = (Column) paramList.getExpressions().get(0);
				}

				for (int i = 0; i < Expressions.size(); i++) {
					Expression expr = (Expression) Expressions.get(i);
					expr.accept(this);
				}
			} else if (paramList == null && fn.isAllColumns()) {
				// handles count(*)
				AggOnColumn = (Column) schema[0];
				type = "int";
			}
		}

		exprName = fn.getName();
		// set alias
	}

	@Override
	public void visit(DoubleValue dv) {
		if (!isProjectCheck) {
			colValue = new doubleDatum((String.valueOf(dv.getValue())));
		}

	}

	@Override
	public void visit(Parenthesis expr) {
		expr.getExpression().accept(this);

	}

	@Override
	public void visit(StringValue sv) {
		if (!isProjectCheck) {
			colValue = new stringDatum((String.valueOf(sv.getValue())));
		}
	}

	@Override
	public void visit(Addition expr) {

		expr.getLeftExpression().accept(this);
		IDatum leftValue = colValue;
		expr.getRightExpression().accept(this);
		IDatum rightValue = colValue;
		if (!isProjectCheck) {
			colValue = leftValue.plus(rightValue);
		}

	}

	@Override
	public void visit(Multiplication expr) {
		expr.getLeftExpression().accept(this);
		IDatum leftValue = colValue;
		expr.getRightExpression().accept(this);
		IDatum rightValue = colValue;
		if (!isProjectCheck && myColumnDetails != null) {
			colValue = leftValue.Multiply(rightValue);
		}
	}

	@Override
	public void visit(Subtraction expr) {
		expr.getLeftExpression().accept(this);
		IDatum leftValue = colValue;
		expr.getRightExpression().accept(this);
		IDatum rightValue = colValue;
		if (!isProjectCheck) {
			colValue = rightValue.Difference(leftValue);
		}
	}

	@Override
	public void visit(AndExpression expr) {

		BinaryExpression bex = (BinaryExpression) expr;
		bex.getLeftExpression().accept(this);
		boolean leftValue;
		boolean rightValue;
		rightValue = resultFlag;
		if (isProjectCheck) {
			rightValue = true;
			// because we want the right expression to be visited too
		}

		if (rightValue) {
			// visit right expression only if left expression is true
			// 'or expressions' come as right expressions
			bex.getRightExpression().accept(this);
			leftValue = resultFlag;
			if (leftValue) {
				resultFlag = true;
			}
		} else {
			resultFlag = false;
		}

	}

	@Override
	public void visit(OrExpression expr) {
		BinaryExpression bex = (BinaryExpression) expr;
		bex.getLeftExpression().accept(this);
		boolean rightValue = resultFlag;
		// visit right expression only if left expression returns false
		if (isProjectCheck) {
			rightValue = false;
			// because we want the right expression to be visited too
		}
		if (rightValue) {
			resultFlag = true;
		} else {
			bex.getRightExpression().accept(this);
			boolean leftValue = resultFlag;
			if (!isProjectCheck) {
				// set resultFlag only if the expression is not visited as part
				// of project operation
				if (leftValue) {
					resultFlag = true;
				} else {
					resultFlag = false;
				}
			}
		}

	}

	@Override
	public void visit(GreaterThan expr) {

		IDatum LeftExprValue = null;
		IDatum RightExprvalue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;

		expr.getRightExpression().accept(this);
		RightExprvalue = colValue;
		if (!isProjectCheck) {
			if (LeftExprValue.compareTo(RightExprvalue) > 0) {
				resultFlag = true;
			} else {
				resultFlag = false;
			}
		}
	}

	@Override
	public void visit(GreaterThanEquals expr) {

		IDatum LeftExprValue = null;
		IDatum RightExprvalue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;

		expr.getRightExpression().accept(this);
		RightExprvalue = colValue;
		if (!isProjectCheck) {
			if (LeftExprValue.compareTo(RightExprvalue) >= 0) {
				resultFlag = true;
			} else {
				resultFlag = false;
			}
		}

	}

	@Override
	public void visit(MinorThan expr) {

		IDatum LeftExprValue = null;
		IDatum RightExprvalue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;

		expr.getRightExpression().accept(this);
		RightExprvalue = colValue;
		if (!isProjectCheck) {
			if (LeftExprValue.compareTo(RightExprvalue) < 0) {
				resultFlag = true;
			} else {
				resultFlag = false;
			}
		}
	}

	@Override
	public void visit(MinorThanEquals expr) {

		IDatum LeftExprValue = null;
		IDatum RightExprvalue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;

		expr.getRightExpression().accept(this);
		RightExprvalue = colValue;
		if (!isProjectCheck) {
			if (LeftExprValue.compareTo(RightExprvalue) <= 0) {
				resultFlag = true;
			} else {
				resultFlag = false;
			}
		}

	}

	@Override
	public void visit(EqualsTo expr) {

		IDatum LeftExprValue = null;
		IDatum RightExprvalue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;

		expr.getRightExpression().accept(this);
		RightExprvalue = colValue;
		if (!isProjectCheck) {
			if (LeftExprValue.compareTo(RightExprvalue) == 0) {
				resultFlag = true;
			} else {
				resultFlag = false;
			}
		}
	}

	@Override
	public void visit(NotEqualsTo expr) {
		IDatum LeftExprValue = null;
		IDatum RightExprvalue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;

		expr.getRightExpression().accept(this);
		RightExprvalue = colValue;
		if (!isProjectCheck) {
			if (LeftExprValue.compareTo(RightExprvalue) == 0) {
				resultFlag = false;
			} else {
				resultFlag = true;
			}
		}
	}
	
	@Override
	public void visit(InExpression expr) {
		IDatum LeftExprValue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;
		
		if (!isProjectCheck) {
			String LeftExprString = LeftExprValue.getValue().toString();
			if(expr.isNot()){
				if (JoinPriority.notInList.contains(LeftExprString)) {
					resultFlag = false;;
				} else {
					resultFlag = true;
				}
			}
			else{
			if (JoinPriority.inList.contains(LeftExprString)) {
				resultFlag = true;
			} else {
				resultFlag = false;
			}
			}
		}
	}

	@Override
	public void visit(IsNullExpression expr) {
		IDatum LeftExprValue = null;

		expr.getLeftExpression().accept(this);
		LeftExprValue = colValue;
		if (!isProjectCheck) {
			if (LeftExprValue == null) {
				resultFlag = true;
			} else {
				resultFlag = false;
			}
		}
	}

}
