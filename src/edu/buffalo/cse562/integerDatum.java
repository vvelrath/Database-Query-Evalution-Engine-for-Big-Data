package edu.buffalo.cse562;

public class integerDatum implements IDatum {

	int i;

	public integerDatum(String s) {
		i = Integer.parseInt(s);
	}

	@Override
	public Object getValue() {
		return this.i;
	}

	@Override
	public boolean equals(IDatum d) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int compareTo(IDatum rightexprDatum) {
		int a = (Integer) this.getValue();
		int b = (Integer) rightexprDatum.getValue();
		if (a > b)
			return 1;
		else if (a < b)
			return -1;
		else
			return 0;
	}

	@Override
	public IDatum plus(IDatum rightValue) {

		int n1 = ((int) this.getValue() + (int) rightValue.getValue());
		IDatum result = new integerDatum(String.valueOf(n1));
		return result;
	}

	@Override
	public IDatum Difference(IDatum leftValue) {

		leftValue = new integerDatum(leftValue.getValue().toString());
		int n1 = ((int) leftValue.getValue() - (int) this.getValue());
		IDatum result = new integerDatum(String.valueOf(n1));
		return result;
	}
	
	@Override
	public IDatum Multiply(IDatum rightValue) {
		int n1 = ((int) this.getValue() * (int) rightValue.getValue());
		IDatum result = new integerDatum(String.valueOf(n1));
		return result;
	}

}
