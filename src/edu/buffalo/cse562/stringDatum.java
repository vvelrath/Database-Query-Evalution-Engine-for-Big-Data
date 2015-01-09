package edu.buffalo.cse562;

public class stringDatum implements IDatum{
	String s;
	public stringDatum(String s){
		this.s = s;
	}
	@Override
	public boolean equals(IDatum d) {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public int compareTo(IDatum rightexprDatum) {
		String a = (String) this.getValue();
		String b = (String) rightexprDatum.getValue();
		int result = a.compareTo(b);
		return result;
	}
	@Override
	public Object getValue() {
		return this.s;
	}
	@Override
	public IDatum plus(IDatum rightValue) {
		String s = ((String) this.getValue() + (String) rightValue.getValue());
		IDatum result = new stringDatum(s);
		return result;
	}
	@Override
	public IDatum Difference(IDatum leftValue) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public IDatum Multiply(IDatum rightValue) {
		// TODO Auto-generated method stub
		//does not apply
		return null;
	}
}
