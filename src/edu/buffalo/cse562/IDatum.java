package edu.buffalo.cse562;


public interface IDatum {

	public boolean equals(IDatum d);

	public int compareTo(IDatum rightexprDatum);

	public Object getValue();

	public IDatum plus(IDatum rightValue);
	public IDatum Difference(IDatum leftValue);
	// parameter 'leftValue' for Difference is intentional (not a typo)
	//Usage : rightValue.Difference(leftValue);
	public IDatum Multiply(IDatum rightValue);

}
