package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;

public  class AggregateCalculator {
	
	public HashMap<String,IDatum> mySumDetails=new HashMap<String,IDatum>();
	public HashMap<String,Integer> myCountDetails=new HashMap<String,Integer>();
	public HashMap<String,Integer> myAVGCountDetails=new HashMap<String,Integer>();
	public HashMap<String,Collection<String>> myDistinctValues=new HashMap<String,Collection<String>>();
	
	
	public IDatum ComputeAggregate(Expression expr,IDatum oldValue,StatementEvaluator evaluator,String key,String aggColName)
	{
		ArrayList<String> DistinctVals =null;
		//retrieve the value in this column and update it according to the aggregate function
Function func=(Function) expr;
	IDatum result=null;
	String newKey=key.concat(aggColName);
	switch(func.getName().toUpperCase()){
	
	case "COUNT": if(myCountDetails.containsKey(newKey))
	{
		if(func.isDistinct()){
			DistinctVals = new ArrayList<String>();
			DistinctVals=(ArrayList<String>) myDistinctValues.get(newKey);
			String val=evaluator.wasMyColumnDetails.get(aggColName).getValue().toString();
			if(!DistinctVals.contains(val)){
				myCountDetails.put(newKey, myCountDetails.get(newKey)+1);
				DistinctVals.add(val);
				myDistinctValues.put(newKey, DistinctVals);
			}
		}
		else{
		myCountDetails.put(newKey, myCountDetails.get(newKey)+1);
		}
		
	}
	else
	{
		if(func.isDistinct()){
			DistinctVals = new ArrayList<String>();
			DistinctVals.add(evaluator.wasMyColumnDetails.get(aggColName).getValue().toString());
			myDistinctValues.put(newKey, DistinctVals);
		}
		myCountDetails.put(newKey, 1);
	}
		
		int n=myCountDetails.put(newKey, myCountDetails.get(newKey));
		result=new integerDatum(String.valueOf(n));
		 break;
		
	case "AVG":
		
		if(mySumDetails.containsKey(newKey))
		{
		
			myAVGCountDetails.put(newKey, myAVGCountDetails.get(newKey)+1);
		String type1= FromScanner.colDetails.get(aggColName);
		switch (type1.toLowerCase()) {

		case "int":
			int n1 = ((int) mySumDetails.get(newKey).getValue() + (int) evaluator.wasMyColumnDetails.get(aggColName).getValue());
			result=new integerDatum(String.valueOf(n1));
			break;
		case "double":
		
			double n2 =((double) mySumDetails.get(newKey).getValue() + (double) evaluator.wasMyColumnDetails.get(aggColName).getValue());
			result=new doubleDatum(String.valueOf(n2));
			break;
		}
		
		mySumDetails.put(newKey,result);
	
			if(type1.toLowerCase() == "int")
	        {    
	            int res=(int)result.getValue();
	            res=res/myAVGCountDetails.get(newKey);
	            result=new integerDatum(String.valueOf(res));
	        }
	        else
	        {    
	            double res=(double)result.getValue();
	            res=res/myAVGCountDetails.get(newKey);
	            result=new doubleDatum(String.valueOf(res));
	        }
		
		}
		else
			
		{
			
			IDatum colValue=evaluator.wasMyColumnDetails.get(aggColName);
			myAVGCountDetails.put(newKey, 1);
			mySumDetails.put(newKey,colValue);
			result=colValue;
		}
		break;
		
	case "SUM":// new value 
		if(mySumDetails.containsKey(newKey))
		{
	IDatum newValue=evaluator.wasMyColumnDetails.get(aggColName);
	String type= FromScanner.colDetails.get(aggColName);
	switch (type.toLowerCase()) {

	case "int":
		int n1 = ((int) newValue.getValue() + (int) oldValue.getValue());
		result=new integerDatum(String.valueOf(n1));
		break;
	case "double":
		double n2 = ((double) newValue.getValue() + (double) oldValue.getValue());
		result=new doubleDatum(String.valueOf(n2));
		break;
	}
		
	}
	
	else
	{
		IDatum newValue=evaluator.wasMyColumnDetails.get(aggColName);
		mySumDetails.put(newKey,newValue);
		result=newValue;
	}

	}
	return result;
}
}
