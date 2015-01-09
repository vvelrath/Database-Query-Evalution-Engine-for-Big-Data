package edu.buffalo.cse562;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import java.util.Map.Entry;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class HybridHJoinOperator implements IOperator {
	IOperator left;
	IOperator right;
	IDatum[] leftValues = null;
	IDatum[] rightValues = null;
	Expression onExpression=null;
	Column[] schema;
	File left_file;
	File right_file;
	
	public static int i=0;
	HashMap<File,File> partitionPairs = null;
	ObjectInputStream left_obj_stream = null;
	ObjectInputStream right_obj_stream = null;
	FileInputStream left_fin_stream = null;
	FileInputStream right_fin_stream = null;
	Iterator<Entry<File, File>> partitions_itr = null;
	HashMap<File,ObjectOutputStream> leftFile_to_stream = null;
	HashMap<File,ObjectOutputStream> rightFile_to_stream = null;
	
	public HybridHJoinOperator(IOperator left, IOperator right,Expression onExpression, Column[] right_schema) {
		this.left = left;
		this.right = right;
		this.onExpression=onExpression;
		schema =null;
		
		partitionPairs = new HashMap<File,File>();
		leftFile_to_stream = new HashMap<File,ObjectOutputStream>();
		rightFile_to_stream = new HashMap<File,ObjectOutputStream>();
			
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
		
		i=i+1;//For naming the files
		
		leftValues = left.readOneTuple();
		
		BinaryExpression bex = (BinaryExpression)onExpression;
		Column leftColumn = (Column) bex.getLeftExpression();
		Column rightColumn = (Column) bex.getRightExpression();
		StatementEvaluator evaluator=new StatementEvaluator(left_schema,leftValues);
		rightColumn.accept(evaluator);
		
		if(evaluator.getColValue()!=null)
		{
			leftColumn = (Column) bex.getRightExpression();
			rightColumn = (Column) bex.getLeftExpression();
		}
		
		
		do {
			
			evaluator=new StatementEvaluator(left_schema,leftValues);
			
			leftColumn.accept(evaluator);
			String leftKey = evaluator.getColValue().getValue().toString();
			int leftHashCode = leftKey.hashCode();
			
			File left_file = new File(Main.swapDir,"HybridHash_left_"+i+"_"+leftHashCode);
			
			try {
				ObjectOutputStream objout = null;
				if(!left_file.exists())
				{
					left_file.createNewFile();
					objout = new ObjectOutputStream(new FileOutputStream(left_file));
				}
				else
				{
					objout = leftFile_to_stream.get(left_file);
				}
					
				objout.writeObject(leftValues);
				leftFile_to_stream.put(left_file, objout);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
					
		} while ((leftValues = left.readOneTuple())!=null);
		
		
		
		ArrayList<ObjectOutputStream> objStream_litr = new ArrayList<ObjectOutputStream>(leftFile_to_stream.values());
		for(int i=0;i<objStream_litr.size();i++)
		{
			try {
				objStream_litr.get(i).flush();
				objStream_litr.get(i).close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(i>=2)
		{
			int j= i-1;
			File[] hash_files = Main.swapDir.listFiles();
			for(File used_file:hash_files)
			{
				if(used_file.getName().startsWith("HybridHash_right_"+j)||used_file.getName().startsWith("HybridHash_left_"+j))
				{
					used_file.delete();
				}	
			}	
		}	
		
			
		rightValues = right.readOneTuple();
		
		do {
			
			evaluator=new StatementEvaluator(right_schema,rightValues);
			rightColumn.accept(evaluator);
			
			String rightKey = evaluator.getColValue().getValue().toString();
			int rightHashCode = rightKey.hashCode();
			
			File left_file = new File(Main.swapDir,"HybridHash_left_"+i+"_"+rightHashCode);
			File right_file = new File(Main.swapDir,"HybridHash_right_"+i+"_"+rightHashCode);
			
			try {
				if(left_file.exists())
				{
					ObjectOutputStream objout = null; 
					if(!right_file.exists())
					{	
						right_file.createNewFile();
						objout = new ObjectOutputStream(new FileOutputStream(right_file));
					}
					else
					{
						objout = rightFile_to_stream.get(right_file);
					}	
					objout.writeObject(rightValues);
					rightFile_to_stream.put(right_file, objout);
					
					right_file = new File(Main.swapDir,"HybridHash_right_"+i+"_"+rightHashCode);
					
					partitionPairs.put(left_file, right_file);
				}
					
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		} while ((rightValues = right.readOneTuple())!=null);
		
		
		ArrayList<ObjectOutputStream> objStream_ritr = new ArrayList<ObjectOutputStream>(rightFile_to_stream.values());
		for(int i=0;i<objStream_ritr.size();i++)
		{
			try {
				objStream_ritr.get(i).flush();
				objStream_ritr.get(i).close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		
		partitions_itr = partitionPairs.entrySet().iterator();
		
		if(partitions_itr.hasNext())
		{	
			Entry<File, File> filePair = partitions_itr.next();
			left_file = filePair.getKey();
			right_file = filePair.getValue();
			try {
				left_fin_stream = new FileInputStream(left_file);
				right_fin_stream = new FileInputStream(right_file);
				left_obj_stream = new ObjectInputStream(left_fin_stream);
				right_obj_stream = new ObjectInputStream(right_fin_stream);
				leftValues = (IDatum[]) left_obj_stream.readObject();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	@Override
	public void resetStream() {
		try {
			right_fin_stream.close();
			right_obj_stream.close();
			right_fin_stream = new FileInputStream(right_file);
			right_obj_stream = new ObjectInputStream(right_fin_stream);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public IDatum[] readOneTuple() {
		IDatum[] colValues=null;
	do{
		
		try
		{
			if(left_obj_stream==null || right_obj_stream == null)
			{
				return null;
			} 
			else if(right_fin_stream.available()>0)
			{
				rightValues = (IDatum[]) right_obj_stream.readObject();	
			}
			else if(left_fin_stream.available()>0)
			{
				leftValues = (IDatum[]) left_obj_stream.readObject();
				this.resetStream();
				rightValues = (IDatum[]) right_obj_stream.readObject();
			}
			else
			{
				left_fin_stream.close();
				right_fin_stream.close();
				
				left_obj_stream.close();
				right_obj_stream.close();
				
				if(partitions_itr.hasNext())
				{
					Entry<File, File> filePair = partitions_itr.next();
					left_file = filePair.getKey();
					right_file = filePair.getValue();
					
					left_fin_stream = new FileInputStream(left_file);
					right_fin_stream = new FileInputStream(right_file);
					
					left_obj_stream = new ObjectInputStream(left_fin_stream);
					right_obj_stream = new ObjectInputStream(right_fin_stream);
					
					leftValues = (IDatum[]) left_obj_stream.readObject();
					rightValues = (IDatum[]) right_obj_stream.readObject();
				}
				else
				{
					left_obj_stream = null;
					right_obj_stream = null;
					return null;
				}
				
			}
		} 
		
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		
	} while(colValues==null);
		
		return colValues;
	}

	@Override
	public Column[] getSchema() {
		// TODO Auto-generated method stub
		//return left.getSchema();
		return this.schema;
	}

	@Override
	public void setSchema(Column[] col) {
		// TODO Auto-generated method stub
		//left.setSchema(col);
		this.schema = col;
	}
	

}
