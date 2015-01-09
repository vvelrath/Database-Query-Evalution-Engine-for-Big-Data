package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;

public class ExternalSort implements IOperator {

	int fileNum = 1;
	IOperator input;
	Column[] schema;
	static PlainSelect pselect;
	public static int size_of_inputbuffer;
	public BufferedReader reader;
	public BufferedReader outputReader;
	public File chunkFile;
	private String tuple;
	private boolean hasTupleFlag;
	static int colNum;
	int result;
	static String aDelimiter = "[|\\n]";
	static String[] types;
	public List<IDatum[]> tuples = new ArrayList<IDatum[]>();
	int[] colPos = null;
	private List<?> orderBy;
	static int k = -1;
	static List<Integer> a = new ArrayList<Integer>();
	List<?> selectCols;
	public HashMap<String,Integer> aggrColIndex = new HashMap<String,Integer>();
	boolean evaluated = false;
	boolean limit_flag = false;
	int limit_rows = 0;
	public static int num_of_attr = 0;
	PriorityQueue<ExternalSort> queue;
	TreeMap<ExternalSort, Integer> map;
	Comparator<IDatum[]> comparator;
	int loc =0;
	
	public ExternalSort(IOperator input, Column[] schema,
			PlainSelect pselect, List<?> orderBy, HashMap<String,Integer> aggrColIndex) {
		// TODO Auto-generated constructor stub
		this.input = input;
		this.schema = schema;
		this.pselect = pselect;
		this.orderBy = orderBy;
		this.aggrColIndex = aggrColIndex;
	}
	     
	public ExternalSort(File f) {
	    try {
	   		reader = new BufferedReader(new FileReader(f), size_of_inputbuffer);	        
	       	this.tuple = reader.readLine();
	       	if(this.tuple == null){
	       		hasTupleFlag = false;
	       	}
	       	else{
	       		hasTupleFlag = true;
	       	}
	       } catch(IOException e) {
	       	// TODO Auto-generated catch block
	       	e.printStackTrace();
	       }
        chunkFile = f;
    }
	     
	public ExternalSort() {
		// TODO Auto-generated constructor stub
	}

	private void getType() {
		types = new String[num_of_attr];
		String type = null;
		int pos = 0;
		if(StatementEvaluator.getAggFuncFlag()==true) {
			for (String s : aggrColIndex.keySet()) {					
				for (String s1 : FromScanner.colIndex.keySet()) {
					if (s.equalsIgnoreCase(s1)) {
						int i = FromScanner.colIndex.get(s1);
						type = FromScanner.colDetails.get(FromScanner.schema[i].getColumnName());
						types[pos] = type;
						continue;
					}
				}
				pos++;
			}
		}
		else {
		//	String[] aValueList = tuple.split(aDelimiter);
			for(int i=0;i<num_of_attr;i++){
				type = FromScanner.colDetails.get(FromScanner.schema[i].getColumnName());
				types[i] = type;
			}
		}
	}
		
	public static IDatum[] readFromFile(String line) {
		if(line!=null) {
       	String[] aValueList = line.split(aDelimiter);
		IDatum[] colValue = new IDatum[aValueList.length];
		int pos=0;
			for(String s: types) {
				switch(s.toLowerCase()) { 
				case "int": colValue[pos] = new integerDatum(aValueList[pos]); break;
				case "boolean": colValue[pos] = new booleanDatum(aValueList[pos]); break;
				case "date": colValue[pos] = new dateDatum(aValueList[pos]); break;
				case "double": colValue[pos] = new doubleDatum(aValueList[pos]); break;
				case "string": colValue[pos] = new stringDatum(aValueList[pos]); break;
				}
				pos++;
			}
		return colValue;
	   	}
		return null;
	}

	public IDatum[] getTuple() {
	       if(hasTupleFlag == false) return null;
	       else {
	       	IDatum[] colValue = readFromFile(tuple);
	       	return colValue;
	       }
	}
	    
	public IDatum[] returnTuple() {
    	IDatum[] answer = getTuple();
    	try {
        	this.tuple = reader.readLine();
        	if(this.tuple == null){
        		hasTupleFlag = false;
        	}
        	else{
        		hasTupleFlag = true;
        	}
      } catch(IOException e) {
    	// TODO Auto-generated catch block
		e.printStackTrace();
      }
      return answer;
    }
	    
    public void sortDataset(final int colNum) {	
		comparator = new Comparator<IDatum[]>()
		{
			@Override
			public int compare(IDatum[] tupleA, IDatum[] tupleB)
			{
				if(tupleA==null || tupleB==null)
					return 0;
				if(tupleA[colNum]==null || tupleB[colNum]==null)
					return 0;				
				return tupleA[colNum].compareTo(tupleB[colNum]);
			}
		};
		Collections.sort(tuples, comparator);
	}
	
	public void reverseTuples() {
		Collections.reverse(tuples);
	}
	    
    public void evaluateOrderBy(){			
		//check if aggregates were evaluated/group by was performed
		if(StatementEvaluator.getAggFuncFlag()==true) {
			for (int i1=0;i1<orderBy.size();i1++) {
				for (String s : aggrColIndex.keySet()) {
					if (orderBy.get(i1).toString().contains(s)) {
						a.add(aggrColIndex.get(s));
						continue;
					}	
				}
			}
			//order by
			for (int i2=a.size()-1; i2>=0; i2--) {
				sortDataset(a.get(i2));
				if(OrderByOperator.ascBuffer.get(i2)==false && SelectEvaluator.orderByFlag==true)
					reverseTuples();						
			}
		}
				
		else {
			for (int i1=0;i1<orderBy.size();i1++) {
				for (String s : FromScanner.colDetails.keySet()) {
					if (orderBy.get(i1).toString().contains(s)) {
						a.add(FromScanner.colIndex.get(s));
						continue;
					}	
				}
			}
			//order by
			for (int i2=a.size()-1; i2>=0; i2--) {
				sortDataset(a.get(i2));
				if(OrderByOperator.ascBuffer.get(i2)==false && SelectEvaluator.orderByFlag==true)
					reverseTuples();						
			}
		}
		evaluated = true;
		if(pselect.getLimit()!=null && SelectEvaluator.orderByFlag==true) {
			limit_rows = (int) pselect.getLimit().getRowCount();
			limit_flag  = true;
		}
	}
	    
    public void chunks_in_mem() {
		//amount of free memory available
		long mem = Runtime.getRuntime().freeMemory();
		//size of each chunk of data
		long chunkSize = mem/2;
		//read each chunk into memory and sort
		ArrayList<IDatum[]> chunck_in_mem = new ArrayList<IDatum[]>();
		ArrayList<File> chunk_files = new ArrayList<File>();
		int num_of_chunks;
			
		IDatum[] colValues = null;
		do {
			long chunk_in_mem_size = 0;
			colValues = input.readOneTuple();
			do {
				chunck_in_mem.add(colValues);
				chunk_in_mem_size = chunk_in_mem_size + colValues.toString().length();
				colValues = input.readOneTuple();
			} while(chunk_in_mem_size<=chunkSize && colValues!=null);
			if(colValues==null && evaluated!=true) {
				OrderByOperator.tuples = chunck_in_mem;
				Main.swapFlag=false;
				return;
			}
				
			File f = null;
			try {
				f = sortChunks(chunck_in_mem);
				chunck_in_mem.clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			chunk_files.add(f);
			chunck_in_mem.clear();
		} while (colValues != null);
		//sort chunk files after merging
		num_of_chunks = chunk_files.size();
		size_of_inputbuffer = (int) (mem/(num_of_chunks+1));
		getType();
		map = new TreeMap<ExternalSort,Integer>(new Comparator<ExternalSort>() {
			public int compare(ExternalSort i, ExternalSort j) {
	        	 IDatum[] arg1 = i.getTuple();
	        	 IDatum[] arg2 = j.getTuple();
	        	 for(int i2=a.size()-1; i2>=0; i2--) {
	        		 colNum = a.get(i2);
	        		 if(OrderByOperator.ascBuffer.get(i2)==false) {
	        			 result = comparator.compare(arg2, arg1);
	        		 } 
	        		 else {
	        			 result = comparator.compare(arg1, arg2);
	        		 }
	        	 }
	        	 return result;
	         }
		});
		for (File f : chunk_files) {
			ExternalSort input_buffer = new ExternalSort(f);
			map.put(input_buffer,loc++);				         
		}
		limit_rows = (int) pselect.getLimit().getRowCount();				
    }

    public File sortChunks(ArrayList<IDatum[]> chunck_in_mem) throws IOException {
		File chunkFile = new File(Main.swapDir,"SortedChunkfile_"+fileNum);
		IDatum[] values = null;
        List<IDatum[]> tempTuples = new ArrayList<IDatum[]>();
       	ArrayList<String> line = new ArrayList<String>();
       	StringBuffer sb = new StringBuffer();
       	chunkFile.createNewFile();
		fileNum++;
        BufferedWriter writer = new BufferedWriter(new FileWriter(chunkFile));
       	
		try {
			tuples = chunck_in_mem;
		if(evaluated!=true) {
			evaluateOrderBy();
		}
		else {
			for (int i2=a.size()-1; i2>=0; i2--) {
				sortDataset(a.get(i2));
				if(OrderByOperator.ascBuffer.get(i2)==false)
					reverseTuples();						
			}
		}
		if(limit_flag==true) {
			limit_rows = (int) pselect.getLimit().getRowCount();
			do{
				k++;
				limit_rows--;
				tempTuples.add(tuples.get(k));
			}while(limit_rows>0);
			k=-1;
			tuples.clear();
			tuples = tempTuples;
		}
		
       	for (int i=0; i<tuples.size(); i++) {
       		num_of_attr=0;
       		values = tuples.get(i);
       		for (int i1 = 0; i1 < values.length; i1++) {
       			num_of_attr++;
       			if (i1 == values.length - 1) {
       				sb.append(values[i1].getValue().toString());
       			}
       			else {
       				sb.append(values[i1].getValue().toString() + "|");
       			}
			}
       		line.add(sb.toString());
       		sb.delete(0, sb.length());
    	}
       	tuples.clear();
       	tempTuples.clear();
       	
        for(String r : line) {
        	writer.write(r);
        	writer.newLine();
        }	  
        line.clear();
        chunkFile.deleteOnExit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally {
			writer.close();
		}
        return chunkFile;
	}

	@Override
	public void resetStream() {
		input.resetStream();
	}

	@Override
	public IDatum[] readOneTuple() {
		if(!evaluated)
			chunks_in_mem();
		if(Main.swapFlag) {
			try {
			if(map.size()!=0) {
				Map.Entry<ExternalSort, Integer> mapEntry = map.pollFirstEntry();
				ExternalSort temp_buffer = mapEntry.getKey();
                IDatum[] tuple = temp_buffer.returnTuple();	
                if(temp_buffer.hasTupleFlag == true) {
                	map.put(temp_buffer,loc++);
                } else {
                	temp_buffer.reader.close();
                    temp_buffer.chunkFile.delete();                	 
                }
                if(limit_flag==true) {
    				if(limit_rows>0) {
    					limit_rows--;
    					return tuple;
    				}
    			}
                else
                	return tuple;
			}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}

	@Override
	public Column[] getSchema() {
		return input.getSchema();
	}

	@Override
	public void setSchema(Column[] col) {
		this.schema = schema;
	}

}