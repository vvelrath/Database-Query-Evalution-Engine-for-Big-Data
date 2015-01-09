package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Iterator;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class SortMergeJoinOperator implements IOperator {
	IOperator left;
	IOperator right;
	IDatum[] leftValues = null;
	IDatum[] rightValues = null;
	int leftIndex;
	int rightIndex;
	Expression onExpression = null;
	Column leftColumn;
	Column rightColumn;
	Column[] schema;
	
	ArrayList<IDatum[]> leftInMemory = null;
	ArrayList<IDatum[]> rightInMemory = null;
	Iterator<IDatum[]> leftIterator = null;
	Iterator<IDatum[]> rightIterator = null;
	
	public SortMergeJoinOperator(IOperator left, IOperator right,Expression onExpression, Column[] right_schema) {
		this.left = left;
		this.right = right;
		this.onExpression=onExpression;
		
		leftIndex = 0;
		rightIndex = 0;
		leftColumn = null;
		rightColumn = null;
		schema = null;	
		
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
		rightInMemory = new ArrayList<IDatum[]>();
		this.left = loadList(left,leftInMemory);
		this.right = loadList(right,rightInMemory);
			
		leftIterator = leftInMemory.iterator();
		rightIterator = rightInMemory.iterator();
		
		leftValues = leftIterator.next();
		rightValues = rightIterator.next();

		
		BinaryExpression bex = (BinaryExpression)onExpression;
		leftColumn = (Column) bex.getLeftExpression();
		rightColumn = (Column) bex.getRightExpression();
		StatementEvaluator evaluator=new StatementEvaluator(left_schema,leftValues);
		rightColumn.accept(evaluator);
		
		if(evaluator.getColValue()!=null)
		{
			leftColumn = (Column) bex.getRightExpression();
			rightColumn = (Column) bex.getLeftExpression();
		}
		
		evaluator = new StatementEvaluator(left_schema,leftColumn);
		leftIndex = evaluator.columnIndex;
		
		evaluator = new StatementEvaluator(right_schema,rightColumn);
		rightIndex = evaluator.columnIndex;
				
	}

	@Override
	public void resetStream() {
	}

	@Override
	public IDatum[] readOneTuple() {
		
		do
		{
			int leftKey = 0;
			int rightKey = 0;
			
			if(rightValues!=null)
			{	
				leftKey = (int) leftValues[leftIndex].getValue();
				rightKey = (int) rightValues[rightIndex].getValue();
			}
			else
				return null;
				
			
			if(leftKey == rightKey)
			{
				break;
			}
			else if(leftKey>rightKey)
			{
				if(rightIterator.hasNext())
				{	
					rightValues = rightIterator.next();
				}
				else
				{
					this.right = loadList(right,rightInMemory);
					if(!rightInMemory.isEmpty())
					{	
						rightIterator = rightInMemory.iterator();
						rightValues = rightIterator.next();
					}
					else
						return null;
				}
			}	
			else
			{
				if(leftIterator.hasNext())
				{	
					leftValues = leftIterator.next();
				}
				else
				{
					this.left = loadList(left,leftInMemory);
					if(!leftInMemory.isEmpty())
					{	
						leftIterator = leftInMemory.iterator();
						leftValues = leftIterator.next();
					}
					else
						return null;
				}
			}
		}while(true);	
		
		
		ArrayList<IDatum> common_list = new ArrayList<IDatum>();
		
		for(int i=0;i<leftValues.length;i++)
		{
			common_list.add(leftValues[i]);	
		}
		
		for(int i=0;i<rightValues.length;i++)
		{
			common_list.add(rightValues[i]);
		}
		
		if(rightIterator.hasNext())
		{	
			rightValues = rightIterator.next();
		}
		else
		{
			this.right = loadList(right,rightInMemory);
			if(!rightInMemory.isEmpty())
			{	
				rightIterator = rightInMemory.iterator();
				rightValues = rightIterator.next();
			}
			else
				rightValues = null;
		}
		
		IDatum[] colValues = new IDatum[common_list.size()];
		colValues = common_list.toArray(colValues);
		
		return colValues;
	}

	@Override
	public Column[] getSchema() {
		// TODO Auto-generated method stub
		return this.schema;
	}

	@Override
	public void setSchema(Column[] col) {
		// TODO Auto-generated method stub
		this.schema = col;;
	}
	
	public IOperator loadList(IOperator input, ArrayList<IDatum[]> recordInMemory)
	{
		recordInMemory.clear();	
		for(int i=0;i<50000;i++)
		{			
			IDatum[] record = input.readOneTuple();

			if(record==null)
				break;
			
			recordInMemory.add(record);
		}
		return input;
	}

	

}
