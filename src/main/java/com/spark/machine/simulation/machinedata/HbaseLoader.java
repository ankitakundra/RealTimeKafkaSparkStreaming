package com.spark.machine.simulation.machinedata;

import java.io.IOException;
import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class HbaseLoader implements Serializable {
	private static final long serialVersionUID = 1L;
	Connection connection = null;
	Table table = null;
	
	public HbaseLoader() throws IOException
	{
						Configuration conf = HBaseConfiguration.create();
						connection = ConnectionFactory
								.createConnection(conf);
	}
	public void hbaseloadmethod(String key,String value) throws IOException
	{
		String values[] = value.split(",");
		table = connection.getTable(TableName.valueOf("machine_data"));
		Put p = new Put(Bytes.toBytes(key));
		p.addColumn(Bytes.toBytes("data"),Bytes.toBytes("name"), Bytes.toBytes(values[0]));
		p.addColumn(Bytes.toBytes("data"),Bytes.toBytes("temperature"),Bytes.toBytes(values[1]));
		table.put(p);
	}					
}
