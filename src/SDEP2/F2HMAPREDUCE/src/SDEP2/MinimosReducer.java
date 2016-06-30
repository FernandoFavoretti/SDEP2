package SDEP2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MinimosReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();

        String dado = conf.get("dado");
        String anoI = conf.get("anoI");
        String anoF = conf.get("anoF");
        String agregacao = conf.get("agregacao");

        ArrayList<Double> dado1 = new ArrayList<Double>();			// A entrada do dado precisa ser "dado1,dado2" 
        ArrayList<Double> dado2 = new ArrayList<Double>();
        int cont = 0;
        for (Text value : values) {								// Separar os dados em duas listas
            cont++;
            String valor = value.toString();
            StringTokenizer valor2 = new StringTokenizer(valor, ",");
            dado1.add(Double.parseDouble(valor2.nextElement().toString()));
            dado2.add(Double.parseDouble(valor2.nextElement().toString()));
        }
        Iterator<Double> dado1Iterator = dado1.iterator();
        double mediax = 0;
        double mediay = 0;
        while (dado1Iterator.hasNext()) {					// Calcular media do primeiro dado
            mediax += dado1Iterator.next();
        }
        mediax = mediax / cont;
        Iterator<Double> dado2Iterator = dado2.iterator(); // Calcular media do segundo dado
        while (dado2Iterator.hasNext()) {
            mediay += dado2Iterator.next();
        }
        mediay = mediay / cont;

        double total1 = 0;
        double total2 = 0;

        for (int z = 0; z < dado2.size(); z++) {
            total1 = total1 + dado1.get(z) * (dado2.get(z) - mediay);		// Somatoria de cima da equacao
            total2 = total2 + dado1.get(z) * (dado1.get(z) - mediax);		// Somatoria debaixo da equacao
        }
        double b = total1 / total2;										// Calculo de B
        double a = mediay - (b * mediax);									// Calculo de A

        // "y = a + bx" representa a equacao resultante dos minimos quadrados
        String mq = dado + " y = " + String.valueOf(a) + " + " + String.valueOf(b) + " * x";

        context.write(key, new Text(mq));

    }

}
