package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class HybridHJoinOperator_Hash implements IOperator {
	IOperator left;
	IOperator right;
	IDatum[] leftValues = null;
	IDatum[] rightValues = null;
	String leftString = null;
	String rightString = null;
	Expression onExpression=null;
	Column[] schema;
	ArrayList<PrintWriter> prStream_litr = null;
	ArrayList<PrintWriter> prStream_ritr = null;
	
	public static int i=0;
	int leftCount;
	int rightCount;
	int leftIndex;
	int rightIndex;
	HashMap<File,File> partitionPairs = null;
	BufferedReader left_buf_reader = null;
	BufferedReader right_buf_reader = null;
	FileReader left_fin_reader = null;
	FileReader right_fin_reader = null;
	Column leftColumn;
	Column rightColumn;
	Iterator<Entry<File, File>> partitions_itr = null;
	HashMap<File,PrintWriter> leftFile_to_stream = null;
	HashMap<File,PrintWriter> rightFile_to_stream = null;
	HashSet<String> fileNames = null;
	
	HashMap<String,ArrayList<IDatum[]>> leftHashMap = null;
	ArrayList<IDatum[]> leftArrayList = null;
	Pattern pipesplitter = null;
	
	public HybridHJoinOperator_Hash(IOperator left, IOperator right,Expression onExpression, Column[] right_schema) {
		this.left = left;
		this.right = right;
		this.onExpression=onExpression;
		schema =null;
		leftColumn = null;
		rightColumn = null;
		leftCount = 0;
		rightCount = 0;
		leftIndex = 0;
		rightIndex = 0;
		pipesplitter = Pattern.compile("\\|");
		
		partitionPairs = new HashMap<File,File>();
		leftFile_to_stream = new HashMap<File,PrintWriter>();
		rightFile_to_stream = new HashMap<File,PrintWriter>();
		leftHashMap = new HashMap<String,ArrayList<IDatum[]>>();
		leftArrayList = new ArrayList<IDatum[]>();
		fileNames = new HashSet<String>();
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
		
		i=i+1; //For naming the files
		
		leftValues = left.readOneTuple();
		
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
		
		
		do {
			String leftKey = leftValues[leftIndex].getValue().toString();
			int leftHashCode = Math.abs(leftKey.hashCode()%32);
			String leftFileName = "HybridHash_left_"+i+"_"+leftHashCode;
			File left_file = new File(Main.swapDir,leftFileName);
			
			try {
				PrintWriter out = null;
				if(!fileNames.contains(leftFileName))
				{
					left_file.createNewFile();
					out = new PrintWriter(new FileWriter(left_file));
					fileNames.add(leftFileName);
					leftFile_to_stream.put(left_file, out);
				}
				else
				{
					out = leftFile_to_stream.get(left_file);
				}
				
				leftString = pipedString(leftValues);
					
				out.println(leftString);
				
				leftCount = leftCount + 1;
				if((leftCount%10000)==0)
				{
					prStream_litr = new ArrayList<PrintWriter>(leftFile_to_stream.values());
					for(int i=0;i<prStream_litr.size();i++)
					{
						prStream_litr.get(i).flush();
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
						
		} while ((leftValues = left.readOneTuple())!=null);
		
		
		prStream_litr = new ArrayList<PrintWriter>(leftFile_to_stream.values());
		for(int i=0;i<prStream_litr.size();i++)
		{
			prStream_litr.get(i).close();
		}
		
			
		rightValues = right.readOneTuple();
		
		do {
			
			String rightKey = rightValues[rightIndex].getValue().toString();
			int rightHashCode = Math.abs(rightKey.hashCode()%32);
			
			String leftFileName = "HybridHash_left_"+i+"_"+rightHashCode;
			String rightFileName = "HybridHash_right_"+i+"_"+rightHashCode;
			
			try {
				if(fileNames.contains(leftFileName))
				{
					File left_file = new File(Main.swapDir,leftFileName);
					File right_file = new File(Main.swapDir,rightFileName);
					PrintWriter out = null;
					
					if(!fileNames.contains(rightFileName))
					{	
						right_file.createNewFile();
						out = new PrintWriter(new FileWriter(right_file));
						fileNames.add(rightFileName);
						rightFile_to_stream.put(right_file, out);
						partitionPairs.put(left_file, right_file);
					}
					else
					{
						out = rightFile_to_stream.get(right_file);
					}	
					
					rightString = pipedString(rightValues);
					out.println(rightString);
					
					rightCount = rightCount + 1;
					if((rightCount%10000)==0)
					{
						prStream_ritr = new ArrayList<PrintWriter>(rightFile_to_stream.values());
						for(int i=0;i<prStream_ritr.size();i++)
						{
							prStream_ritr.get(i).flush();
						}
					}
				}
				
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		} while ((rightValues = right.readOneTuple())!=null);
		
		
		prStream_ritr = new ArrayList<PrintWriter>(rightFile_to_stream.values());
		for(int i=0;i<prStream_ritr.size();i++)
		{
			prStream_ritr.get(i).close();
		}
		
		
		
		partitions_itr = partitionPairs.entrySet().iterator();
		
		if(partitions_itr.hasNext())
		{	
			Entry<File, File> filePair = partitions_itr.next();
			File left_file = filePair.getKey();
			File right_file = filePair.getValue();
			try {
				left_fin_reader = new FileReader(left_file);
				right_fin_reader = new FileReader(right_file);
				left_buf_reader = new BufferedReader(left_fin_reader);
				right_buf_reader = new BufferedReader(right_fin_reader);
				
				leftString = left_buf_reader.readLine();
				
				do
				{
					leftValues = stringToDatum(leftString,left_schema);
					String leftKey = leftValues[leftIndex].getValue().toString();
					
					ArrayList<IDatum[]> leftList = null;
					if(leftHashMap.containsKey(leftKey))
					{
						leftList = leftHashMap.get(leftKey);
						leftList.add(leftValues);
					}
					else
					{
						leftList = new ArrayList<IDatum[]>();
						leftList.add(leftValues);
					}
					leftHashMap.put(leftKey,leftList);
				}
				while((leftString = left_buf_reader.readLine())!=null);
				
				
				String rightKey = null;
				leftString = null;
				leftValues = null;
				
				rightString = right_buf_reader.readLine();
				
				do
				{	
					rightValues = stringToDatum(rightString,right_schema);	
					rightKey = rightValues[rightIndex].getValue().toString();
					
					if(leftHashMap.containsKey(rightKey))
					{
						leftArrayList = new ArrayList<IDatum[]>();
						leftArrayList.addAll(leftHashMap.get(rightKey));
					}
				}while(!leftHashMap.containsKey(rightKey)&&((rightString = right_buf_reader.readLine())!=null));
				
				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
			
	}

	@Override
	public void resetStream() {
		right.resetStream();
	}

	@Override
	public IDatum[] readOneTuple() {
		IDatum[] colValues=null;
	do{
		
		try
		{
			if(left_buf_reader==null || right_buf_reader == null)
			{
				return null;
			}
			else if(leftArrayList.size()>0)
			{
				leftValues = leftArrayList.get(0);
				leftArrayList.remove(0);
			}	
			else if((rightString = right_buf_reader.readLine())!=null)
			{
				String rightKey = null;
				do
				{
					rightValues = stringToDatum(rightString,right.getSchema());	
					rightKey = rightValues[rightIndex].getValue().toString();
					
					if(leftHashMap.containsKey(rightKey))
					{
						leftArrayList = new ArrayList<IDatum[]>();
						leftArrayList.addAll(leftHashMap.get(rightKey));
						leftValues = leftArrayList.get(0);
						leftArrayList.remove(0);
					}
				}while(!leftHashMap.containsKey(rightKey)&&(rightString = right_buf_reader.readLine())!=null);
			}
				
		
			if(rightString==null && leftArrayList.size()==0 && leftValues==null)
			{
				left_fin_reader.close();
				right_fin_reader.close();
				left_buf_reader.close();
				right_buf_reader.close();
				
				leftHashMap = new HashMap<String,ArrayList<IDatum[]>>();

				if(partitions_itr.hasNext())
				{
					do
					{	
						Entry<File, File> filePair = partitions_itr.next();
						File left_file = filePair.getKey();
						File right_file = filePair.getValue();
						
						left_fin_reader = new FileReader(left_file);
						right_fin_reader = new FileReader(right_file);
						
						left_buf_reader = new BufferedReader(left_fin_reader);
						right_buf_reader = new BufferedReader(right_fin_reader);
						
						leftString = left_buf_reader.readLine();
						do
						{
							leftValues = stringToDatum(leftString,left.getSchema());
							String leftKey = leftValues[leftIndex].getValue().toString();
							ArrayList<IDatum[]> leftList = null;
							if(leftHashMap.containsKey(leftKey))
							{
								leftList = leftHashMap.get(leftKey);
								leftList.add(leftValues);
							}
							else
							{
								leftList = new ArrayList<IDatum[]>();
								leftList.add(leftValues);
							}	
							leftHashMap.put(leftKey,leftList);
						}
						while((leftString = left_buf_reader.readLine())!=null);
						
						
						String rightKey = null;
						leftValues = null;
						leftString = null;
						
						rightString = right_buf_reader.readLine();
						do
						{	
							rightValues = stringToDatum(rightString,right.getSchema());
							rightKey = rightValues[rightIndex].getValue().toString();
							
							if(leftHashMap.containsKey(rightKey))
							{
								leftArrayList = new ArrayList<IDatum[]>();
								leftArrayList.addAll(leftHashMap.get(rightKey));
								leftValues = leftArrayList.get(0);
								leftArrayList.remove(0);
							}
						}while(!leftHashMap.containsKey(rightKey)&&(rightString = right_buf_reader.readLine())!=null);
					
						if(leftValues==null)
						{
							left_fin_reader.close();
							right_fin_reader.close();
							left_buf_reader.close();
							right_buf_reader.close();
						}	
						
					}while(leftValues==null && partitions_itr.hasNext());
					
					if(leftValues==null)
					{
						left_fin_reader.close();
						right_fin_reader.close();
						left_buf_reader.close();
						right_buf_reader.close();
						
						left_buf_reader = null;
						right_buf_reader = null;
						return null;
					}	
				}
				else
				{
					left_buf_reader = null;
					right_buf_reader = null;
					return null;
				}	
			}	
			
		} 
		
		catch (IOException e) {
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
		leftValues=null;
		
	} while(colValues==null);
		
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
		this.schema = col;
	}
	
	public String pipedString(IDatum[] values)
	{
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < values.length; i++) {					
			if (i == values.length - 1)
				str.append(values[i].getValue());
			else
			{
				str.append(values[i].getValue());
				str.append("|");
			}
		}	
		return str.toString();
		
	}
	
	
	public IDatum[] stringToDatum(String pipeString,Column[] schema_tmp) {

		String[] aValueList = pipesplitter.split(pipeString);
		IDatum[] colValue = new IDatum[aValueList.length];
		String type = null;
		
		for(int i=0;i<aValueList.length;i++){
			
			type = FromScanner.colDetails.get(schema_tmp[i].getColumnName());
			
			switch(type) { 
			case "int": colValue[i] = new integerDatum(aValueList[i]); break;
			case "boolean": colValue[i] = new booleanDatum(aValueList[i]); break;
			case "date": colValue[i] = new dateDatum(aValueList[i]); break;
			case "double": colValue[i] = new doubleDatum(aValueList[i]); break;
			case "string": colValue[i] = new stringDatum(aValueList[i]); break;
			}
		}
		return colValue;

	}
	

}
