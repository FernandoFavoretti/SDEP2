package SDEP2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MediaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)	 throws IOException, InterruptedException {

		
		Configuration conf = context.getConfiguration();
		
		String dado = conf.get("dado");
		String anoI = conf.get("anoI");
		String anoF = conf.get("anoF");
		String agregacao = conf.get("agregacao");
		
		if(dado.equals("temp")){
			double sum = 0;
            int cont = 0;
			for (DoubleWritable value : values) {
			cont++;
			sum +=  value.get();
			
	 }
			
			Double avg = (double) sum/cont;

	 context.write(key, new DoubleWritable(avg));

		}
		
	}
	
}


