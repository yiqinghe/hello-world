package com.test.bolt;


import java.util.Random;

/**
 * Created by caigaonian870 on 16/10/21.
 */
public class Test {
	public static void main(String[] args) {
		Random random = new Random();
		System.out.println(Float.valueOf((random.nextFloat()*10000)).longValue());
		//调用
		test();
	}
	public static void test(){
		System.out.println("test");
	}
}
