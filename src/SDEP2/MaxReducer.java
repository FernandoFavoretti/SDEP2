package SDEP2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MaxReducer  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)	 throws IOException, InterruptedException {
	
	
	double maxValue = Double.MIN_VALUE;

	 for (DoubleWritable value : values) {

	 maxValue = Math.max(maxValue, value.get());

	 }

	 context.write(key, new DoubleWritable(maxValue));

	}

}
