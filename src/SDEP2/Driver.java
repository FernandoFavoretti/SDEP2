package SDEP2;

import java.util.Scanner;

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
		String[] info = new String[8]		
				
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
	 info[0] = args[0];
	 
	 while (ok){
	 System.out.println ("Bem vindo ao F2H Analyser! Vamos Começar?");
	 System.out.println("Qual dado você deseja analisar hoje?");
	 System.out.println("temp- Temperatura");
	 System.out.println("ca- Pontos de Condesacao da Agua");
	 System.out.println("pn- Pressao no nivel do mar");
	 System.out.println("pe- Pressao estacionaria");
	 System.out.println("vb- Visibilidade");
	 System.out.println("vv- Velocidade do Vento");
	 System.out.println("rv- Rajadas de Vento");
	 System.out.println("pr- Precipitacao");
	 System.out.println("nv- Neve");
	 info[1] = scan.next();
	 
	 System.out.println("Sobre qual intervalo de tempo você deseja analisar "+indiceDado+ "?");
	 System.out.println("Temos informações desde 1910 ate 2016!");
	 System.out.print("Ano Inicial:"); info[2]= Integer.parseInt(scan.next());
	 System.out.print("Ano Final:"); info[3]= Integer.parseInt(scan.next());
	 
	 System.out.println("-----Loading-------");
	 
	 System.out.println("Qual informação você deseja para os parametros escolhidos?");
	 System.out.println("media - Media");
	 System.out.println("mdn   - Mediana");
	 System.out.println("max   - Valor maximo");
	 System.out.println("min   - Valor minimo");
	 System.out.println("moda  - Moda");
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
	 	  

	 Driver driver = new Driver();

	 int exitCode = ToolRunner.run(driver, args);

	 System.exit(exitCode);
	 }
     
	 


	 }
	
	

}


