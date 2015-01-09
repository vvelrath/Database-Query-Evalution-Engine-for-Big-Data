package edu.buffalo.cse562;


import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.create.table.*;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;

public class FromScanner implements FromItemVisitor{

	File basePath;
	HashMap<String,CreateTable> tables;
	HashMap<String, ArrayList<String>> requiredColsInTables;
	//public static HashMap<Integer,String> colType = new HashMap<Integer,String>();
	public static HashMap<String,String> colDetails = new HashMap<String,String>();
	public static HashMap<String,Integer> colIndex = new HashMap<String,Integer>();
	public static Column[] schema=null;
	public IOperator source=null;
	
	public FromScanner(File basePath,HashMap<String,CreateTable> tables,HashMap<String, ArrayList<String>> requiredColsInTables)
	{
	this.basePath=basePath;
	this.tables=tables;
	this.requiredColsInTables=requiredColsInTables;
	}
	
	public FromScanner(File basePath,HashMap<String,CreateTable> tables)
	{
	this.basePath=basePath;
	this.tables=tables;
	this.requiredColsInTables=new HashMap<String, ArrayList<String>>();
	}
	
	@Override
	public void visit(Table tableName) {
		//instantiate a 'createTable' instance with the table 'tableName' from the hash map
		ArrayList<String> requiredCols=null;
		String tabName=null;
		int size=0;
		HashMap<String,String> isColRequired = new HashMap<String,String>();
		
	CreateTable table=tables.get(tableName.getName().toUpperCase());
	List<?> cols=table.getColumnDefinitions();
	
	if(tableName.getAlias()!=null){
		tabName=tableName.getAlias();
	}
	else{
		tabName=tableName.getName();
	}
	if(!requiredColsInTables.isEmpty()){
	requiredCols=new ArrayList<String>(requiredColsInTables.get(tabName));
	size=requiredCols.size();
	}
	
	 
		
	schema=new Column[cols.size()];
	for(int i=0;i<cols.size();i++)
	{
		ColumnDefinition colDef=(ColumnDefinition)cols.get(i);
		
		String type = colDef.getColDataType().toString();
		
		if(type.toLowerCase().startsWith("decimal"))
		{
			ColDataType dataType = new ColDataType();
			dataType.setDataType("double");
			colDef.setColDataType(dataType);
		}
				
		if(type.toLowerCase().startsWith("varchar")||type.toLowerCase().startsWith("char"))
		{
			ColDataType dataType = new ColDataType();
			dataType.setDataType("string");
			colDef.setColDataType(dataType);
		}
		
	//	colType.put(i,colDef.getColDataType().toString());
		colDetails.put(colDef.getColumnName(), colDef.getColDataType().toString().toLowerCase());
		colIndex.put(colDef.getColumnName(), i);
		// populate the schema array to contain column names 
		schema[i]=new Column(tableName,colDef.getColumnName());
		
		String colName=schema[i].getColumnName();
		String wholeColName2=schema[i].getWholeColumnName();
		String wholeColName1=tabName+"."+colName;
	
		if(!requiredColsInTables.isEmpty()){
		if(requiredCols.contains(wholeColName1)){
			isColRequired.put(wholeColName2,"yes");
		}else{
			isColRequired.put(wholeColName2,"no");
		}
		}
	}
	//create a class instead of the one below
	source=new FileReadOperator(new File(basePath,tableName.getName()+".dat"),colDetails, schema, isColRequired,size);
		this.schema=source.getSchema();
	}

	@Override
	public void visit(SubSelect subselect) {
		// TODO Auto-generated method stub
		SelectBody select = (SelectBody)subselect.getSelectBody();
		
		PlainSelect pselect = null;
		
		if (select instanceof PlainSelect) {

			pselect = (PlainSelect) select;
			SelectEvaluator sel_visitor = new SelectEvaluator(source, basePath, tables);
			pselect.accept(sel_visitor);
			source=sel_visitor.operator;
			this.schema=source.getSchema();
		}
		
		List<?> selectItems = pselect.getSelectItems();
        for(int i=0;i<selectItems.size();i++)
        {
            SelectExpressionItem select_expr = (SelectExpressionItem) selectItems.get(i);
            StatementEvaluator evaluator = new StatementEvaluator();
            Expression expr = select_expr.getExpression();
            expr.accept(evaluator);
            
            if(select_expr.getAlias()!=null)
                colDetails.put(select_expr.getAlias(), evaluator.getType().toLowerCase());
        }
	}

	@Override
	public void visit(SubJoin arg0) {
		// TODO Auto-generated method stub
		
	}

}
