package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class JoinOperator implements IOperator {
	IOperator left;
	IOperator right;
	Column[] schema;
	IDatum[] leftValues = null;
	IDatum[] rightValues = null;
	Expression onExpression=null;
	
	ArrayList<IDatum[]> leftInMemory = null;
	ArrayList<IDatum[]> rightInMemory = null;
	Iterator<IDatum[]> leftIterator = null;
	Iterator<IDatum[]> rightIterator = null;
	
	public JoinOperator(IOperator left, IOperator right,Expression onExpression, Column[] right_schema) {
		this.left = left;
		this.right = right;
		this.onExpression=onExpression;
		schema=null;
		
		ArrayList<Column> schlst = new ArrayList<Column>();
		Column[] left_schema = left.getSchema();
		for(int i=0;i<left_schema.length;i++)
		{
			schlst.add(left_schema[i]);
		}
		for(int i=0;i<right_schema.length;i++)
		{
			schlst.add(right_schema[i]);
		}
		Column[] schema_loc = new Column[schlst.size()];
		schema_loc = schlst.toArray(schema_loc);
		this.setSchema(schema_loc);
		
		
		leftInMemory = new ArrayList<IDatum[]>();
		
		this.left = loadList(left,leftInMemory);
		
		if(!leftInMemory.isEmpty())
		{	
			leftIterator = leftInMemory.iterator();
			leftValues = leftIterator.next();
		}
        else
            leftInMemory = null;
	
		rightInMemory = new ArrayList<IDatum[]>();
		this.right = loadList(right,rightInMemory);
		rightIterator = rightInMemory.iterator();
	
	}

	@Override
	public void resetStream() {
		right.resetStream();
	}

	@Override
	public IDatum[] readOneTuple() {
		IDatum[] colValues=null;
	do{
		
		if(leftInMemory==null || rightInMemory == null)
		{
			return null;
		}
		else if(rightIterator.hasNext())
		{
			rightValues = rightIterator.next();
		}
		else if(!leftInMemory.isEmpty() && leftIterator.hasNext())
		{
			leftValues = leftIterator.next();
			rightIterator = rightInMemory.iterator();
			rightValues = rightIterator.next();
		}
		else
		{	
			this.right = loadList(right,rightInMemory);
			
			if(!rightInMemory.isEmpty())
			{	
				rightIterator = rightInMemory.iterator();
				rightValues = rightIterator.next();
				leftIterator = leftInMemory.iterator();
				leftValues = leftIterator.next();
			}
			else
			{
				loadList(left,leftInMemory);	
				
				if(!leftInMemory.isEmpty())
				{
					resetStream();
					loadList(right,rightInMemory);
					rightIterator = rightInMemory.iterator();
					rightValues = rightIterator.next();
					leftIterator = leftInMemory.iterator();
					leftValues = leftIterator.next();
				}
				else
				{
					leftInMemory.clear();
					rightInMemory.clear();
					leftInMemory = null;
					rightInMemory = null;
					return null;
				}		
			}
		}
		
		
		
		ArrayList<IDatum> commonList = new ArrayList<IDatum>();
		
		
		for(int i=0;i<leftValues.length;i++)
		{
			commonList.add(leftValues[i]);	
		}
		
		
		for(int i=0;i<rightValues.length;i++)
		{
			commonList.add(rightValues[i]);
		}
		
		
		colValues = new IDatum[commonList.size()];
		colValues = commonList.toArray(colValues);
		
		StatementEvaluator evaluator=new StatementEvaluator(this.getSchema(),colValues);
		onExpression.accept(evaluator);
		if (!(evaluator.getResult() == true)) {
			colValues = null;
		}
		
		
	} while(colValues==null);
		
		return colValues;
	}

	@Override
	public Column[] getSchema() {
		return this.schema;
	}

	@Override
	public void setSchema(Column[] col) {
		this.schema = col;
	}
	
	public IOperator loadList(IOperator input, ArrayList<IDatum[]> recordInMemory)
	{
		recordInMemory.clear();	
		for(int i=0;i<1000;i++)
		{			
			IDatum[] record = input.readOneTuple();

			if(record==null)
				break;
			
			recordInMemory.add(record);
		}
		return input;
	}
	

}
