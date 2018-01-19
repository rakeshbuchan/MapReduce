package com.mapreduce.assignment1;

public class Util {

	// Fibonacci utility
	/*
	 * public static int Fibonacci(int fib) { if (fib == 0) return 0; else { if
	 * (fib == 1) return 1; else { return (Fibonacci(fib - 1) + Fibonacci(fib -
	 * 2)); } } }
	 */
	public static void Fibonacci(int value) {
		int x = 0, y = 1, z = 1;
		for (int i = 0; i < value; i++) {
			x = y;
			y = z;
			z = x + y;
		}
	}
}
