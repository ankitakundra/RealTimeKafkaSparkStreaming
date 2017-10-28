package com.spark.machine.simulation.machinedata;

import java.io.Serializable;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class HbaseConvertToPut implements PairFunction<Tuple2<String,String>,ImmutableBytesWritable, Put>,Serializable {

	private static final long serialVersionUID = 1L;

	public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, String> arg0)
			throws Exception {
		String values[] = arg0._2.split(",");
		Put p = new Put(Bytes.toBytes(arg0._1));
		p.addColumn(Bytes.toBytes("data"),Bytes.toBytes("name"), Bytes.toBytes(values[0]));
		p.addColumn(Bytes.toBytes("data"),Bytes.toBytes("temperature"),Bytes.toBytes(values[1]));
		return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(),p);
	}

}
