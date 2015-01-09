package edu.buffalo.cse562;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;

//override Comparable interface 

public class Main {
	/**
	 * @param args
	 */
	public static boolean swapFlag = false;
	public static File swapDir = null;

	public static void dump(IOperator input) {
		IDatum[] values = null;
		PrintWriter pr = null;		
		// TODO: Vivek: check if we are printing in to the file  as expected: filename and other things
		try {
		/*	if(swapFlag == true) {
				outputFile = input.readLargeChunks(swapDir);
			}
			else {*/
			pr = new PrintWriter("nbaexpected.dat");
			do {
				values = input.readOneTuple();
				if (values == null)
					return;
				for (int i = 0; i < values.length; i++) {					
					if (i == values.length - 1) {
						System.out.print(values[i].getValue());
						pr.print(values[i].getValue()+"\n");
					} else {						
						System.out.print(values[i].getValue() + "|");
						pr.print(values[i].getValue() + "|");
					}
				}

				System.out.println();

			} while (values != null);
//			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			pr.close();
		}
	}

	public static void main(String[] args) throws ParseException {
		int i;
		File dataDir = null;
		IOperator operator = null;
		ArrayList<File> sqlFiles = new ArrayList<File>();
		HashMap<String, CreateTable> tables = new HashMap<String, CreateTable>();

		for (i = 0; i < args.length; i++) {
			if (args[i].equals("--data")) {
				dataDir = new File(args[i + 1]);
				i++;
			} 
			else if (args[i].equals("--swap")) {
				swapFlag = true;
				swapDir = new File(args[i+1]);
				i++;
			}
			else {
				sqlFiles.add(new File(args[i]));
			}
		}

		for (File sql : sqlFiles) {
			try {
				FileReader stream = new FileReader(sql);
				CCJSqlParser parser = new CCJSqlParser(stream);
				Statement stmt;

				while ((stmt = parser.Statement()) != null) {

					if (stmt instanceof CreateTable) {
						CreateTable ct = (CreateTable) stmt;
						tables.put(ct.getTable().getName(), ct);

					} else if (stmt instanceof Select) {

						SelectBody select = ((Select) stmt).getSelectBody();

						if (select instanceof PlainSelect) {

							PlainSelect pselect = (PlainSelect) select;
							
							JoinPriority jp=new JoinPriority();
							SelectEvaluator sel_visitor = new SelectEvaluator(operator,dataDir, tables);
							
							String st=	JoinPriority.reWriteStatement(pselect);
						
							String stat=st.replaceAll("\\[", "").replaceAll("\\]", "");
							StringReader strReader = new StringReader(stat);
							parser = new CCJSqlParser(strReader);
							Statement stmt_new = parser.Statement();
							PlainSelect pselect_new = (PlainSelect)((Select) stmt_new).getSelectBody();
												
							pselect_new.accept(sel_visitor);
							
							operator=sel_visitor.operator;
						}
					}
				}
				dump(operator);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

	}
}
