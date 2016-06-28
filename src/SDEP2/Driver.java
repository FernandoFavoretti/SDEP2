package SDEP2;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
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
	
		
		 //Info
			//0 = caminho dos arquivos AGREGADOS
			//1 = dado
			//2 = ano inicial
			//3 = ano final
			//4 = metodo
			//5 = tipo de agregacao
			//6 = pasta output
	    
		Configuration conf = getConf();
		conf.set("dado", args[1]);
		conf.set("anoI", args[2]);
		conf.set("anoF", args[3]);
		conf.set("metodo", args[4]);
		conf.set("agregacao", args[5]);	
		
		Job job = Job.getInstance(conf);

	 job.setJarByClass(Driver.class);

	 job.setJobName("Results para"+ args[1]);

	 int anoIni = 0;
	 
	 for(anoIni = Integer.parseInt(args[2]); anoIni <= Integer.parseInt(args[3]); anoIni++){
		 if(args[4].equals("media")){
	    	MultipleInputs.addInputPath(job, new Path(args[0]+"/"+anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
		 }else
		 if(args[4].equals("max")){
		    	MultipleInputs.addInputPath(job, new Path(args[0]+"/"+anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
		 }
		 if(args[4].equals("min")){
		    	MultipleInputs.addInputPath(job, new Path(args[0]+"/"+anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
		 }
		 if(args[4].equals("var")){
		    	MultipleInputs.addInputPath(job, new Path(args[0]+"/"+anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
		 }
		 if(args[4].equals("desvio")){
		    	MultipleInputs.addInputPath(job, new Path(args[0]+"/"+anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
		 }
		 if(args[4].equals("mq")){
		    	MultipleInputs.addInputPath(job, new Path(args[0]+"/"+anoIni), CombinedInputFormat.class, MinimosMapper.class);
			 }
     }
	 
	
	 FileOutputFormat.setOutputPath(job,new Path(args[6]));

	 switch (args[4]){
	 case "media": 	 
		 job.setMapperClass(SimpleStatisticsMapper.class);

		 job.setReducerClass(MediaReducer.class);

		 job.setOutputKeyClass(Text.class);

		 job.setOutputValueClass(DoubleWritable.class);
		 
		 break;
	 case "max":
		 job.setMapperClass(SimpleStatisticsMapper.class);

		 job.setReducerClass(MaxReducer.class);

		 job.setOutputKeyClass(Text.class);

		 job.setOutputValueClass(DoubleWritable.class);
		 
		 break;
	 case "min":
		 job.setMapperClass(SimpleStatisticsMapper.class);

		 job.setReducerClass(MinReducer.class);

		 job.setOutputKeyClass(Text.class);

		 job.setOutputValueClass(DoubleWritable.class);
		 
		 break;
	 case "var":
		 job.setMapperClass(SimpleStatisticsMapper.class);

		 job.setReducerClass(VarianciaReducer.class);

		 job.setOutputKeyClass(Text.class);

		 job.setOutputValueClass(DoubleWritable.class);
		 
		 break;
	 case "desvio":
		 job.setMapperClass(SimpleStatisticsMapper.class);

		 job.setReducerClass(DesvioReducer.class);

		 job.setOutputKeyClass(Text.class);

		 job.setOutputValueClass(DoubleWritable.class);
		 
		 break;
	 case "mq": 	 
		 job.setMapperClass(MinimosMapper.class);

		 job.setReducerClass(MinimosReducer.class);
		 
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		 job.setOutputKeyClass(Text.class);

		 job.setOutputValueClass(Text.class);
		 
		 break;
	
	 }
	 

	 System.exit(job.waitForCompletion(true) ? 0:1); 

	 boolean success = job.waitForCompletion(true);

	 return success ? 0 : 1;
	 

	 }

	public static void main(String[] args) throws Exception {
		String[] info = new String[7];		
				
	Scanner scan = new Scanner(System.in);
	
	 //Args
	 //0 = Caminho dos arquivos AGREGADOS
		
	 //Info
		//0 = caminho dos arquivos AGREGADOS
		//1 = dado
		//2 = ano inicial
		//3 = ano final
		//4 = metodo
		//5 = tipo de agregacao
		//6 = pasta output
	 
	 boolean ok = true;
	 //info[0] = args[0];
	 /*
	 while (ok){
	 System.out.println ("Bem vindo ao F2H Analyser! Vamos Começar?");
	 System.out.println("Qual dado você deseja analisar hoje?");
	 System.out.println("temp- Temperatura");
	 System.out.println("ca- Pontos de Condesacao da Agua");
	 System.out.println("pn- Pressao no nivel do mar");
	 System.out.println("pr- Pressao");
	 System.out.println("vb- Visibilidade");
	 System.out.println("vv- Velocidade do Vento");
	 System.out.println("rv- Rajadas de Vento");
	 System.out.println("pr- Precipitacao");
	 System.out.println("nv- Neve");
	 info[1] = scan.next();
	 
	 System.out.println("Sobre qual intervalo de tempo você deseja analisar "+info[1]+ "?");
	 System.out.println("Temos informações desde 1910 ate 2016!");
	 System.out.print("Ano Inicial:"); info[2]= scan.next();
	 System.out.print("Ano Final:"); info[3]= scan.next();
	 
	 System.out.println("-----Loading-------");
	 
	 System.out.println("Qual informação você deseja para os parametros escolhidos?");
	 System.out.println("media - Media");
	 System.out.println("max   - Valor maximo");
	 System.out.println("min   - Valor minimo");
	 System.out.println("var   - Variancia");
	 System.out.println("desvio- Desvio Padrão");
	 System.out.println("mq - Minimos Quadrados");
	 info[4]= scan.next();
	 
	 System.out.println("-----Loading-------");
	 System.out.println("Como voce deseja que o resultado seja agregado?");
	 System.out.println("semana - Segregar por semana");
	 System.out.println("semana -Agregar por dia");
	 System.out.println("ano - Agregar por ano");
	 info[5] = scan.next();
	 
	 System.out.println("Digite o nome da PASTA do arquivo de saida (Nao repita nomes por favor");
	 info[6] = scan.next();
	 
	 
	 System.out.println("========================================");
	 System.out.println("Iniciando analise:");
	 System.out.println("Analisar:"+ info[1]);
	 System.out.println("De "+ info[2] +"Ate "+ info[3]);
	 System.out.println("Utilizando o metodo: "+ info[4]);
	 System.out.println("Agregando seus resultados por: "+ info[5]);
	 System.out.println("O resultado da analise pode ser encontrado em:" +info[6]);
	 System.out.println("========================================");
	*/
	 
	 info[0] = "/home/hduser/Documents/separetedData/gsod/resumedData/";
	 info[1] = "temp";
	 info[2] = "1940";
	 info[3] = "1942";
	 info[4] = "desvio";
	 info[5] = "mesano";
	 info[6] = "output";

	 Driver driver = new Driver();

	 int exitCode = ToolRunner.run(driver, info);

	 System.exit(exitCode);
	 //}
     
	 


 }
	
	
}


