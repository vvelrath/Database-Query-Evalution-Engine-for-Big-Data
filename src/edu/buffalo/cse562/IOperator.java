package edu.buffalo.cse562;

import net.sf.jsqlparser.schema.Column;

interface IOperator {
	public void resetStream();
	public IDatum[] readOneTuple();
	public Column[] getSchema();
	public void setSchema(Column[] col);
}
