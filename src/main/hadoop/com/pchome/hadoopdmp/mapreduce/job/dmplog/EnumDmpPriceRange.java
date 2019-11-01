package com.pchome.hadoopdmp.mapreduce.job.dmplog;
//2001~3000
public enum EnumDmpPriceRange {
	A(0,500),
	B(500,999),
	C(1000,1999),
	D(2000,2999),
	E(3000,3999),
	F(4000,4999),
	G(5000,5999),
	H(6000,6999),
	I(7000,7999),
	J(8000,8999),
	K(9000,9999),
	L(10000,19999),
	M(20000,29999),
	N(30000,39999),
	O(40000,49999),
	P(50000,999999999);
	
	private final int min;
	private final int max;
	
	private EnumDmpPriceRange (int min, int max){
		this.min = min;
		this.max = max;
	}

	public int getMin() {
		return min;
	}

	public int getMax() {
		return max;
	}
	
	
}
