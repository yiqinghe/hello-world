package com.test;


import backtype.storm.topology.TopologyBuilder;

import java.util.Random;


/**
 * Created by caigaonian870 on 16/10/21.
 */
public class hello extends Thread {

	private String hellos;

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		Random rsddddom = new Random(20);
		boolean nextBoolean = rsddddom.nextBoolean();

		test();
	}

	/**
	 *
	 */
	public static void test() {
		System.out.println("xx");
		for (int i = 0; i < 10; i++) {

		}

	}

	@Override
	public void run() {
		super.run();
	}
}
