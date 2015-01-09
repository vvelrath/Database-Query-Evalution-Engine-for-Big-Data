package edu.buffalo.cse562;

public class doubleDatum implements IDatum{

	Double d;
	public doubleDatum(String s){
		d = Double.parseDouble(s);
	}
		@Override
		public boolean equals(IDatum d) {
			// TODO Auto-generated method stub
			return false;
		}
		@Override
		public int compareTo(IDatum rightexprDatum) {
			Double a = (Double) this.getValue();
			Double b = (Double) rightexprDatum.getValue();
			if (a>b) return 1;
			else if(a<b) return -1;
			else return 0;
		} 
		@Override
		public Object getValue() {
			return this.d;
		}
		@Override
		public IDatum plus(IDatum rightValue) {
			double n1 = ((double) this.getValue() + (double) rightValue.getValue());
			IDatum result = new doubleDatum(String.valueOf(n1));
			return result;
		}
	
		@Override
		public IDatum Difference(IDatum leftValue) {
			
			leftValue = new doubleDatum(leftValue.getValue().toString());
			
			double n1 = ((double) leftValue.getValue() - (double) this.getValue());
			n1 = (double)Math.round(n1*100)/100;
			IDatum result = new doubleDatum(String.valueOf(n1));
			return result;
		}
		@Override
		public IDatum Multiply(IDatum rightValue) {
			double n1 = ((double) this.getValue() * (double) rightValue.getValue());
			IDatum result = new doubleDatum(String.valueOf(n1));
			return result;
		}

}
