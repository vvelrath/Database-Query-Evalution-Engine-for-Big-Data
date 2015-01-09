package edu.buffalo.cse562;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SubSelect;

public class JoinPriority {
	public static HashMap<String, Integer> tablePriority = null;
	public static HashMap<Integer, String> tablePriority2 = null;
	public static HashMap<Integer, List<String>> aliasPriority = null;
	//public static List<String> inList=new ArrayList<String>();
	public static HashSet<String> inList=new HashSet<String>();
	public static HashSet<String> notInList=new HashSet<String>();
	
	public JoinPriority() {
		tablePriority = new HashMap<String, Integer>();
		aliasPriority = new HashMap<Integer, List<String>>();
		tablePriority2 = new HashMap<Integer, String>();
		
		
		if(!Main.swapFlag){
		tablePriority.put("region", 8);
		tablePriority.put("nation", 7);
		tablePriority.put("supplier", 6);
		tablePriority.put("part", 5);
		tablePriority.put("customer", 4);
		tablePriority.put("orders", 3);
		tablePriority.put("partsupp", 2);
		tablePriority.put("lineitem", 1);
		
		}else{
		
		tablePriority.put("region", 1);
		tablePriority.put("nation", 2);
		tablePriority.put("supplier", 3);
		tablePriority.put("part", 4);
		tablePriority.put("customer", 5);
		tablePriority.put("orders", 6);
		tablePriority.put("partsupp", 7);
		tablePriority.put("lineitem", 8);
		}


	}
	
	private static void updateJoinPriority(){
		tablePriority.clear();
		tablePriority.put("region",6 );
		tablePriority.put("nation", 1);
		tablePriority.put("supplier",2 );
		tablePriority.put("part",7 );
		tablePriority.put("customer", 5);
		tablePriority.put("orders",4 );
		tablePriority.put("partsupp", 8);
		tablePriority.put("lineitem", 3);
	}

	private static List<Integer> rewriteFromItems(PlainSelect pselect) {
	
		List<Integer> items = new ArrayList<Integer>();
	
		if (pselect.getFromItem() != null) {
			FromItem fItem = pselect.getFromItem();
			String astr=fItem.toString();
			int i = 0;
			if (pselect.getJoins() != null) {

				List<?> joins = pselect.getJoins();
				List<String> alist=null;

				for (; i < joins.size(); i++) {
					Join aJoin = (Join) joins.get(i);
					String aName = aJoin.toString();
					if (aJoin.toString().contains(" AS ")) {
						Matcher matcher = Pattern.compile(".*(?= AS )").matcher(
								aJoin.toString());
						matcher.find();
						aName = matcher.group();
					}
					
					Integer k = tablePriority.get(aName);

					items.add(k);
					alist=new ArrayList<String>();
					if(!aliasPriority.containsKey(k)){
						alist.add(aJoin.toString());
					aliasPriority.put(k,alist);
					}
					else{
						alist=aliasPriority.get(k);
						alist.add(aJoin.toString());
						aliasPriority.put(k, alist);
					}
				}
			}

			if (fItem.toString().contains(" AS ")) {
				Matcher matcher = Pattern.compile(".*(?= AS )").matcher(
						fItem.toString());
				matcher.find();
				astr = matcher.group();
			}
			
		int l = tablePriority.get(astr);
		items.add(l);
		List<String> blist=new ArrayList<String>();
			if(!aliasPriority.containsKey(l)){
				blist.add(fItem.toString());
			aliasPriority.put(l,blist);
			}
			else{
				blist=aliasPriority.get(l);
				blist.add(fItem.toString());
				aliasPriority.put(l, blist);
			}
			
			Collections.sort(items, new CollectionSort());
		}

		return items;

	}
	
	private static String rewriteWhere(String whereExpr) {

		ArrayList<String> whereConditions = new ArrayList<String>();
		ArrayList<String> notInConditions = new ArrayList<String>();
		StringBuilder whereClause=new StringBuilder();
		Scanner sc = new Scanner(whereExpr);
		String inCondition=null;
		sc.useDelimiter("AND");

		while (sc.hasNext()) {
			String condition = sc.next();

			if (condition.contains(" OR ")) {
				inCondition=rewriteOrCondition(condition);
				whereConditions.add(inCondition);
			}else if(condition.contains(" <> ")){
				notInConditions.add(condition);
				
			}
			else {
				whereConditions.add(condition);
			}
		}
		
		String notIn= rewriteNotEqualTo(notInConditions);
		if(notIn==null){
			whereConditions.addAll(notInConditions);
		}
		else{
		whereConditions.add(notIn);
		}
		sc.close();
		
		for(int i=0;i<whereConditions.size();i++){
			whereClause.append(whereConditions.get(i));
			if(i<whereConditions.size()-1){
			whereClause.append(" AND ");
			}
		}
		
		
		return whereClause.toString();
	}

	
	private static String rewriteNotEqualTo(ArrayList<String> notInConditions){
		int j=0;
		StringBuilder inValues=new StringBuilder();
		StringBuilder notInClause=new StringBuilder();
		
		List<String> alist=new ArrayList<String>();
		for(int i=0;i<notInConditions.size();i++){
			String colName=null;
			Matcher matcher = Pattern.compile(".*(?=<>)").matcher(
					notInConditions.get(i));
			matcher.find();
			colName = matcher.group();
			if(!alist.contains(colName.trim())){
				alist.add(colName.trim());
			}
			
		}
		
		if(alist.size()==1){
			boolean flag=false;
			
			
			for(int i=0;i<notInConditions.size();i++){
				if(flag){
					inValues.append(",");
				}
				
				String colVal=null;
				Matcher matcher = Pattern.compile("(?<=<>).*").matcher(
						notInConditions.get(i));
				matcher.find();
				colVal = matcher.group();
				String str1=colVal.replaceAll("'", "");
				notInList.add(str1.trim());
				inValues.append(colVal);
				flag=true;
			}
			notInClause.append("(").append(alist.get(0)).append(" NOT IN ").append("(").append(inValues).append("))");
			
			return notInClause.toString();
		}
		else{
			return null;
		}
		
		
	}
	
	
	private static String rewriteOrCondition(String condition){
		String colName=null;
		StringBuilder inValues=new StringBuilder();
		String stat=condition.replaceAll("\\(", "").replaceAll("\\)", "");
		Scanner sc = new Scanner(stat);
		sc.useDelimiter("OR");
		//List<String> orSubconds=new ArrayList<>();
		boolean flag=false;
		
		while(sc.hasNext()){
			
			if(flag){
				inValues.append(",");
			}
			
			String c=	sc.next();
			if(!flag){
				Matcher matcher = Pattern.compile(".*(?= = )").matcher(
						c);
				matcher.find();
				colName = matcher.group();
				flag=true;
			}
			
			Matcher matcher1 = Pattern.compile("(?<==).*").matcher(
					c);
			matcher1.find();
			String val = matcher1.group();
			String str1=val.replaceAll("'", "");
			inList.add(str1.trim());
			inValues.append(val);
		}
		sc.close();
		
		StringBuilder inClause=new StringBuilder();
		inClause.append("(").append(colName).append(" IN ").append("(").append(inValues).append("))");
		return inClause.toString();
		
	}
	
	public static String reWriteStatement(PlainSelect pselect) {

		boolean skipFlag=false;
		int skipCount=0;
		StringBuilder sql = new StringBuilder("SELECT ");
		if (pselect.getDistinct() != null) {
			sql.append(pselect.getDistinct()).append(" ");
		}

		sql.append(pselect.getSelectItems());
		
		FromItem fItem = pselect.getFromItem();
		
		if(fItem instanceof SubSelect){
			SubSelect subsel=(SubSelect) fItem;
			updateJoinPriority();
			PlainSelect pselect_new = (PlainSelect)subsel.getSelectBody();
			String subSelSql=reWriteStatement(pselect_new);
			sql.append(" FROM ").append(" ( ").append(subSelSql).append(" ) ").append(" AS ").append(subsel.getAlias());
		}
		
		else{
		
			List<Integer> items = rewriteFromItems(pselect);

		if (pselect.getFromItem() != null) {
			List<String> alist=aliasPriority.get(items.get(0));
			sql.append(" FROM ").append((aliasPriority.get(items.get(0))).get(0));
			if(alist.size()>1){
				skipFlag=true;
				for(int i=1;i<alist.size();i++){
				sql.append(", ").append(aliasPriority.get(items.get(0)).get(i));
				skipCount++;
				}
			}
		}
		if (pselect.getJoins() != null) {
			int i = 1;
			if(skipFlag){
				i=i+skipCount;
				skipCount=0;
				skipFlag=false;
			}
			
			for (; i < items.size(); i++) {
								
				List<String> alist=aliasPriority.get(items.get(i));
				sql.append(", ").append((aliasPriority.get(items.get(i))).get(0));
				
				if(alist.size()>1){
					
					for(int k=1;k<alist.size();k++){
					sql.append(", ").append(aliasPriority.get(items.get(i)).get(k));
					i++;
					}
				}
				
			}
		}
		}
		
		if (pselect.getWhere() != null) {
			String where=pselect.getWhere().toString();
			if(where.contains("OR")){
				String newWhere=rewriteWhere(where);
				
				StringReader strReader = new StringReader(newWhere);
			CCJSqlParser parser = new CCJSqlParser(strReader);
				
				try {
					sql.append(" WHERE ").append(parser.Expression());
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
			sql.append(" WHERE ").append(pselect.getWhere());
			}
		}
		if (pselect.getGroupByColumnReferences() != null) {
			sql.append(" GROUP BY ").append(
					pselect.getGroupByColumnReferences());
		}
		if (pselect.getHaving() != null) {
			sql.append(" HAVING ").append(pselect.getHaving());
		}
		if (pselect.getOrderByElements() != null) {
			sql.append(" ORDER BY ").append(pselect.getOrderByElements());
		}
		if (pselect.getLimit() != null) {
			sql.append(pselect.getLimit());
		}

		return sql.toString();

	}
}

