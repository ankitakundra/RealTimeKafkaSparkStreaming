package com.spark.machine.simulation.machinedata;

import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.mapred.JobConf;

public class HbaseLoader implements Serializable {
	private static final long serialVersionUID = 1L;
	String tableName = "machine_data";
	
	public JobConf HbaseConnection() throws IOException
	{
						Configuration conf = HBaseConfiguration.create();
						conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
						JobConf job = new JobConf(new Configuration(), this.getClass()); 
						job.set("mapreduce.output.fileoutputformat.outputdir", "/user/cloudera/output");
						job.setOutputFormat(TableOutputFormat.class);
						job.set(TableOutputFormat.OUTPUT_TABLE, tableName);
						return job;
	}					
}
