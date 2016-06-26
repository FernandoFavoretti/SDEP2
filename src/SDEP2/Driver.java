package SDEP2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*This class is responsible for running map reduce job*/

public class Driver extends Configured implements Tool{
	
	public int run(String[] args) throws Exception{ 

		
		
	 String input = "/home/hduser/Documents/separetedData/gsod/resumedData/1940";	
	 String input2 = "/home/hduser/Documents/separetedData/gsod/resumedData/1941";
	 String output = "outuput";	
	 Job job = new Job();

	 job.setJarByClass(Driver.class);

	 job.setJobName("Teste bigFile");

	 MultipleInputs.addInputPath(job, new Path(input), CombinedInputFormat.class, testMapper.class);
	 MultipleInputs.addInputPath(job, new Path(input2), CombinedInputFormat.class, testMapper.class);
	 

	 FileOutputFormat.setOutputPath(job,new Path(output));

	 job.setMapperClass(testMapper.class);

	 job.setReducerClass(testReducer.class);

	 job.setOutputKeyClass(Text.class);

	 job.setOutputValueClass(DoubleWritable.class);

	 System.exit(job.waitForCompletion(true) ? 0:1); 

	 boolean success = job.waitForCompletion(true);

	 return success ? 0 : 1;

	 }

	public static void main(String[] args) throws Exception {

	 Driver driver = new Driver();

	 int exitCode = ToolRunner.run(driver, args);

	 System.exit(exitCode);

	 }

}


