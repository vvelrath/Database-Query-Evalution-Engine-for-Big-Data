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
import java.util.Iterator;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;

public class OrderByOperator implements IOperator {
		IOperator input;
		IOperator operator;
		Column[] schema;
		static PlainSelect pselect;
		public static List<IDatum[]> tuples = new ArrayList<IDatum[]>();
		int[] colPos = null;
		private List<?> orderBy;
		static int k = -1;
		public static List<Boolean> ascBuffer = new ArrayList<Boolean>();
		static List<Integer> a = new ArrayList<Integer>();
		List<?> selectCols;
		public static HashMap<String,Integer> aggrColIndex = new HashMap<String,Integer>();
		static boolean evaluated = false;
		static boolean limit_flag = false;
		boolean externalSortFlag = false;
		static int limit_rows = 0;
		
		public OrderByOperator(IOperator input, Column[] schema, PlainSelect pselect) {
			this.input = input;
			this.schema = schema;
			this.pselect = pselect;
			selectCols = pselect.getSelectItems();
			orderBy = pselect.getOrderByElements();
			//check if ASC or DESC
			for (Iterator iterate = orderBy.iterator(); iterate.hasNext();) {
                   OrderByElement element = (OrderByElement) iterate.next();
                   if(!element.isAsc()) { ascBuffer.add(false); }
                   else { ascBuffer.add(true); }			                       
			}
			for (int i=0;i<selectCols.size();i++) {
				SelectExpressionItem select_expr = (SelectExpressionItem) selectCols.get(i);
				if(select_expr.getAlias()!=null) {
					aggrColIndex.put(select_expr.getAlias(), i);
				}
				else {
					aggrColIndex.put(selectCols.get(i).toString(), i);
				}
			}
			
		}

		public OrderByOperator() {
			// TODO Auto-generated constructor stub
		}

		public void sortDataset(final int colNum) {	
			Comparator<IDatum[]> comparator = new Comparator<IDatum[]>()
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
			if(Main.swapFlag!=true && tuples.isEmpty()) {
				IDatum[] colValues = null;
				do {
					colValues = input.readOneTuple();
					if(colValues!=null) tuples.add(colValues);
				} while (colValues != null);
			}
			
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
					if(ascBuffer.get(i2)==false)
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
					if(ascBuffer.get(i2)==false)
						reverseTuples();						
				}
			}
			evaluated = true;
			if(pselect.getLimit()!=null) {
				limit_rows = (int) pselect.getLimit().getRowCount();
				limit_flag  = true;
			}
		}
		
		public IDatum[] returnTuples() {	
				//LIMIT
				if(limit_flag==true) {
					k++;
					if(limit_rows>0 && k<tuples.size()) {
						limit_rows--;
						return tuples.get(k);
					}
				}
				else{
					k++;
					/*if(k<=tuples.size())*/
					if(k<tuples.size())
						return tuples.get(k);
				}
			return null;
		}

		@Override
		public void resetStream() {
			input.resetStream();			
		}

		@Override
		public IDatum[] readOneTuple() {
			IDatum[] orderByOutput = null;
			if (Main.swapFlag == true && externalSortFlag==false && !evaluated) {
				operator = new ExternalSort(input, schema, pselect, orderBy, aggrColIndex);
				externalSortFlag = true;
			}
			if(Main.swapFlag == true && externalSortFlag==true) {
				orderByOutput = operator.readOneTuple();
			}
			if (!evaluated) {
				evaluateOrderBy();
			}
			if(Main.swapFlag == false) {
			orderByOutput = returnTuples();
			}
			return orderByOutput;
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
