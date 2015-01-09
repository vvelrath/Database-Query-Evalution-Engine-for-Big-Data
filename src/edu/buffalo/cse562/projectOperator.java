package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class projectOperator implements IOperator {

	IOperator input;
	List<?> selectItems;
	Column[] schema;
	HashMap<String, Integer> schema_map;
	IDatum[] colValues = null;

	public HashMap<String, String> aliasToTable_stmteval = new HashMap<String, String>();
	public HashMap<String, String> columnToTable_stmteval = new HashMap<String, String>();
	String[] colwithTableNames = null;
	String[] colNames = null;
	
	public HashMap<String,String> aliasToTable=new HashMap<String,String>();
	public HashMap<String,String> columnToTable=new HashMap<String,String>();
	public HashMap<String, String> column_keys_sel = new HashMap<String, String>();


	public projectOperator(IOperator input, Column[] schema, List<?> selectItems) {
		this.input = input;
		this.schema = input.getSchema();
		this.selectItems = selectItems;
		this.schema_map = new HashMap<String, Integer>();

		
		for (int i = 0; i < schema.length; i++) {
			Column col=schema[i];
			Table tab=col.getTable();
			schema_map.put(tab.getName()+"|"+col.getColumnName(), i);
			schema_map.put(tab.getAlias()+"|"+col.getColumnName(), i);
			aliasToTable.put(tab.getAlias(), tab.getName());
			columnToTable.put(col.getColumnName(), tab.getName());
						
		}

			Column[] new_schema = new Column[selectItems.size()];
			for (int j = 0; j < selectItems.size(); j++)
			{
				SelectExpressionItem select_expr = (SelectExpressionItem) selectItems.get(j);
				if(select_expr.getAlias()!=null)
					new_schema[j] = new Column(schema[0].getTable(),select_expr.getAlias());
				else
				{
					Column col=(Column)select_expr.getExpression();
					new_schema[j] = new Column(schema[0].getTable(),col.getColumnName());
				}
			}
			this.setSchema(new_schema);
			
			colwithTableNames = new String[input.getSchema().length];
			colNames = new String[input.getSchema().length];
			
			for (int i = 0; i < input.getSchema().length; i++) {
				Column col = input.getSchema()[i];
				Table tab = col.getTable();
				colwithTableNames[i] = tab.getName() + "|" + col.getColumnName();
				colNames[i] = col.getColumnName();
				aliasToTable_stmteval.put(tab.getAlias(), tab.getName());
				columnToTable_stmteval.put(col.getColumnName(), tab.getName());
			}
			
			populateColKeysforSelCols();

	}

	private void populateColKeysforSelCols() {
		for (int j = 0; j < selectItems.size(); j++) {

			SelectExpressionItem sEItem = (SelectExpressionItem) selectItems
					.get(j);
			Expression expr = sEItem.getExpression();

			if (expr instanceof Column) {
				Column col = (Column) expr;
				String col_name = col.getColumnName();
				String table_or_alias = col.getTable().getName();
				StringBuilder column_key_tmp = new StringBuilder();

				if (table_or_alias == null) {
					column_key_tmp.append(columnToTable.get(col_name));
				} 
				else {
					column_key_tmp.append(table_or_alias);
				}
				column_key_tmp.append("|");
				column_key_tmp.append(col_name);
				column_keys_sel.put(table_or_alias+"|"+col_name,column_key_tmp.toString());
				
			}
		}
	}
	
	@Override
	public void resetStream() {
		
		input.resetStream();
	}

	@Override
	public IDatum[] readOneTuple() {

		ArrayList<IDatum> tuple_list = new ArrayList<IDatum>();
		IDatum[] tuple = new IDatum[tuple_list.size()];
		
		do {
			colValues = input.readOneTuple();
				
			if (colValues == null) {
				// has reached EOF
			
				return null;
			}

			//StatementEvaluator evaluator = new StatementEvaluator(input.getSchema(),colValues);
			StatementEvaluator evaluator=new StatementEvaluator(input.getSchema(),colValues,colNames,colwithTableNames,aliasToTable_stmteval,columnToTable_stmteval);
			
			for (int j = 0; j < selectItems.size(); j++) {
				SelectExpressionItem select_expr = (SelectExpressionItem) selectItems
						.get(j);
				Expression expr = select_expr.getExpression();

				if((expr instanceof BinaryExpression)||(expr instanceof Parenthesis))
				{	
					
					expr.accept(evaluator);
					tuple_list.add(evaluator.getColValue());
				}
				else
				{	
					Column col = (Column) select_expr.getExpression();
										
				String Column_key=column_keys_sel.get(col.getTable().getName()+"|"+col.getColumnName());
				tuple_list.add(colValues[schema_map.get(Column_key)]);
					
				}
				
				

			}
			
		} while (tuple_list == null);
				
		tuple = tuple_list.toArray(tuple);
		return tuple;
	}
	
	@Override
	public Column[] getSchema() {
	
		return this.schema;
	}
	@Override
	public void setSchema(Column[] col) {
	
		this.schema=col;
	}
}