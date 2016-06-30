package SDEP2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SimpleStatisticsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        String dado = conf.get("dado");
        String anoI = conf.get("anoI");
        String anoF = conf.get("anoF");
        String agregacao = conf.get("agregacao");

        Dictionary dic = new Dictionary();
        int[] agreg = new int[2];
        if (agregacao.equals("ano")) {
            //Dicionario para retornar a posicao dos anos no arquivo
            agreg = dic.translateAno();
        } else if (agregacao.equals("mes")) {
            agreg = dic.translateMes();
        } else if (agregacao.equals("dia")) {
            agreg = dic.translateSemana();
        } else if (agregacao.equals("mesano")) {
            agreg = dic.translateMesAno();
        } else if (agregacao.equals("messem")) {
            agreg = dic.translateMesSemana();
        } else if (agregacao.equals("estacao")) {
            agreg = dic.translateEstacao();
        }
        //Dicionario para retornar a posicao do dado escolhido no arquivo
        int[] dados = new int[2];
        dados = dic.translateDado(dado);

        String line = value.toString();

        double var = 0.0;
        double missing = 0.0;

        if (line.charAt(0) != 'S') {
            String group = line.substring(agreg[0], agreg[1]);
            var = Double.parseDouble(line.substring(dados[0], dados[1]));
            switch (dado) {
                case "vb":
                    missing = 999.9;
                    break;
                case "vv":
                    missing = 999.9;
                    break;
                case "rv":
                    missing = 999.9;
                    break;
                case "pr":
                    missing = 99.9;
                    break;
                case "nv":
                    missing = 999.9;
                    break;
                default:
                    missing = 9999.9;
                    break;

            }
            {

                if (var != missing) {
                    context.write(new Text(group), new DoubleWritable(var));
                }
                return;
            }

        }

    }
}
