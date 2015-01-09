package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;

import net.sf.jsqlparser.schema.Column;

public class FileReadOperator implements IOperator {

	BufferedReader input;
	File f;
	// HashMap<Integer,String> colType;
	HashMap<String, String> colDetails;
	HashMap<String, String> isColRequired=null;
	Column[] schema = null;
	Column[] schema_original = null;
	int sizeOfColValues;
	Pattern pipesplitter = null;
	IDatum[] colValue= null;
	String[] aValueList = null;
	String[] schema_whole = null;
	String[] type = null;
	
	public FileReadOperator(File f, HashMap<String, String> colDetails,
			Column[] schema_original, HashMap<String, String> isColRequired,
			int sizeOfColValues) {
		this.f = f;
		// this.colType = colType;
		this.colDetails = colDetails;
		if(!(isColRequired.isEmpty())){
			this.isColRequired = isColRequired;
			this.sizeOfColValues = sizeOfColValues;
			schema = new Column[sizeOfColValues];
			
			for (int i = 0, k = -1; i < schema_original.length; i++) {
				String colName = schema_original[i].getWholeColumnName();
				if ((isColRequired.get(colName)).equals("yes")) {
					++k;
					schema[k] = schema_original[i];
				}
			}
		}
		else{
			schema=schema_original;
		}
		
		resetStream();
		this.schema_original = schema_original;
		
		schema_whole = new String[schema_original.length];
		type = new String[schema_original.length];
		for(int i=0;i<schema_original.length;i++)
		{
			schema_whole[i] = schema_original[i].getWholeColumnName();
			type[i] = colDetails.get(schema_original[i].getColumnName());
		}	
		
		
		this.setSchema(schema);
		pipesplitter = Pattern.compile("\\|");
	}

	@Override
	public void resetStream() {
		try {
			input = new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			input = null;
		}

	}

	@Override
	public IDatum[] readOneTuple() {
		if (input == null)
			return null;
		String tuple = null;
		//String type = null;
		//String aDelimiter = "[|\\n]";
		try {
			tuple = input.readLine();
			// TODO: handle EOF
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (tuple == null)
			return null;
		if (tuple.isEmpty()) {
			return readOneTuple();
		}
		//String[] aValueList = tuple.split(aDelimiter);
		colValue = new IDatum[sizeOfColValues];
		aValueList = null;
		aValueList = pipesplitter.split(tuple);
		
		if(!isColRequired.isEmpty()){
			
		//IDatum[] colValue = new IDatum[sizeOfColValues];
		int k = -1;
		for (int i = 0; i < aValueList.length; i++) {

			//String colName = schema_original[i].getWholeColumnName();
			String colName = schema_whole[i];
			// convert only those values to IDatum that are required by the
			// query
			if (isColRequired.get(colName).equals("yes")) {
				++k;
				//type = colDetails.get(schema_original[i].getColumnName());
				switch (type[i]) {
				case "int":
					colValue[k] = new integerDatum(aValueList[i]);
					break;
				case "boolean":
					colValue[k] = new booleanDatum(aValueList[i]);
					break;
				case "date":
					colValue[k] = new dateDatum(aValueList[i]);
					break;
				case "double":
					colValue[k] = new doubleDatum(aValueList[i]);
					break;
				case "string":
					colValue[k] = new stringDatum(aValueList[i]);
					break;
				}
			}
		}
		return colValue;
		}
		
		else{
			//IDatum[] colValue = new IDatum[aValueList.length];
			
			for (int i = 0; i < aValueList.length; i++) {

			
					//type = colDetails.get(schema_original[i].getColumnName());
					switch (type[i]) {
					case "int":
						colValue[i] = new integerDatum(aValueList[i]);
						break;
					case "boolean":
						colValue[i] = new booleanDatum(aValueList[i]);
						break;
					case "date":
						colValue[i] = new dateDatum(aValueList[i]);
						break;
					case "double":
						colValue[i] = new doubleDatum(aValueList[i]);
						break;
					case "string":
						colValue[i] = new stringDatum(aValueList[i]);
						break;
					}
				
			}
			return colValue;
			
		}
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
}
