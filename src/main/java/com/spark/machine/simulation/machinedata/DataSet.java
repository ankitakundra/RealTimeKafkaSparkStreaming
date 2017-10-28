package com.spark.machine.simulation.machinedata;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

public class DataSet {
	String data = null;
	public static void main(String args[]) throws Exception {
		String names[] = { "EVE-100-WALL-E", "HARLIE-189-MQ-TY",
				"SOPHIE-143-SPD", "SHROUD-560-V" };
		Random r = new Random();
		int low = 20;
		int high = 200;
		int i = 1;
		while (i > 0)
			try {
				{
					int temperature = r.nextInt(high - low) + low;
					String machine_id = RandomStringUtils.randomAlphanumeric(6);
					int index = r.nextInt(names.length);
					String machine_names = names[index];
					String data = machine_id + "," + machine_names + ","
							+ temperature;
					System.out.println(data);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
