package SDEP2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MediaMapper  extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
		Configuration conf = context.getConfiguration();
			
		String dado = conf.get("dado");
		String anoI = conf.get("anoI");
		String anoF = conf.get("anoF");
		String agregacao = conf.get("agregacao");
		
		Dictionary dic = new Dictionary();
		
		//Dicionario para retornar ap osicao dos anos no arquivo
		int[] anos = new int[2];
		anos = dic.translateAno();
		
		//Dicionario para retornar a posicao do dado escolhido no arquivo
		int[] dados = new int[2];
		dados = dic.translateDado(dado);
				
		String line = value.toString();
		
		if(dado.equals("temp")){
			double temp;
		
		if(agregacao.equals("ano"))	
			if(line.charAt(0)!='S'){
				String year = line.substring(anos[0],anos[1]);
				temp = Double.parseDouble(line.substring(dados[0], dados[1]));
				context.write(new Text(year), new DoubleWritable(temp));
				return;
			}
		}
}
	
}
