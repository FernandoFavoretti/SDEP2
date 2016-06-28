package SDEP2;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinimosMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	
		Configuration conf = context.getConfiguration();
			
		String dado = conf.get("dado");
		String anoI = conf.get("anoI");
		String anoF = conf.get("anoF");
		String agregacao = conf.get("agregacao");
		
		Dictionary dic = new Dictionary();
		StringTokenizer dataTokenizer = new StringTokenizer(dado, "+");	// A entrada do dado precisa ser "TipoDeDado1+TipoDeDado2"
		String[] dadosSep = new String[2];
		int x = 0;
		while (dataTokenizer.hasMoreElements()) {
			dadosSep[x] = (String) dataTokenizer.nextElement();
			x++;
			
		}
		
		//Dicionario para retornar ap osicao dos anos no arquivo
		int[] anos = new int[2];
		anos = dic.translateAno();
		
		//Dicionario para retornar a posicao do dado escolhido no arquivo
		int[] dados1 = new int[2];
		int[] dados2 = new int[2];
		dados1 = dic.translateDado(dadosSep[0]);
		dados2 = dic.translateDado(dadosSep[1]);	
		
		String line = value.toString();

		double temp;
		double temp2;
		
		if(agregacao.equals("ano"))	
			if(line.charAt(0)!='S'){
				String year = line.substring(anos[0],anos[1]);
				temp = Double.parseDouble(line.substring(dados1[0], dados1[1]));
				temp2 = Double.parseDouble(line.substring(dados2[0], dados2[1]));
				context.write(new Text(year), new Text(String.valueOf(temp)+","+String.valueOf(temp2)));
				return;	// Escrever para o Reduce as 2 chaves com "," entre eles
		}
	}
}
