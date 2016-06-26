package SDEP2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class testMapper extends

Mapper<LongWritable, Text, Text, DoubleWritable> {

@Override

public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

String line = value.toString();
double temp;
if(line.charAt(0)!='S'){
	String year = line.substring(14, 18);
	temp = Double.parseDouble(line.substring(24, 30));
	context.write(new Text(year), new DoubleWritable(temp));

}



}

}


