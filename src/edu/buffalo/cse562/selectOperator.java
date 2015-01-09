package edu.buffalo.cse562;

import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

public class selectOperator implements IOperator {
	IOperator input;
	Expression condition;
	Column[] schema;
	IDatum[] colValues = null;
	public HashMap<String, String> aliasToTable = new HashMap<String, String>();
	public HashMap<String, String> columnToTable = new HashMap<String, String>();
	String[] colwithTableNames = null;
	String[] colNames = null;
	
	public selectOperator(IOperator input,Column[] schema,Expression condition) {
		this.input = input;
		this.condition = condition;
		this.schema=schema;
		colwithTableNames = new String[schema.length];
		colNames = new String[schema.length];
		
		for (int i = 0; i < schema.length; i++) {
			Column col = schema[i];
			Table tab = col.getTable();
			colwithTableNames[i] = tab.getName() + "|" + col.getColumnName();
			colNames[i] = col.getColumnName();
			aliasToTable.put(tab.getAlias(), tab.getName());
			columnToTable.put(col.getColumnName(), tab.getName());
		}
		
	}

	@Override
	public void resetStream() {
		input.resetStream();
	}

	@Override
	public IDatum[] readOneTuple() {
		
		colValues = null;
				
		do {
			colValues = input.readOneTuple();
			if (colValues == null) {
				//has reached EOF
				return null;
			} 
			
			//StatementEvaluator evaluator=new StatementEvaluator(schema,colValues);
			StatementEvaluator evaluator=new StatementEvaluator(schema,colValues,colNames,colwithTableNames,aliasToTable,columnToTable);
			condition.accept(evaluator);
			if (!(evaluator.getResult() == true)) {
				colValues = null;
			}
			
		} while (colValues == null);
		return colValues;
	}
	
	@Override
	public Column[] getSchema() {
		// TODO Auto-generated method stub
		return input.getSchema();
	}

	@Override
	public void setSchema(Column[] col) {
		// TODO Auto-generated method stub
		//this.setSchema(col);
		this.schema=col;
	}
	
}
