package SDEP2;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class VarianciaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        DecimalFormat df = new DecimalFormat("0.##");

        Configuration conf = context.getConfiguration();
        List<Double> aux = new ArrayList<Double>();
        String dado = conf.get("dado");
        String anoI = conf.get("anoI");
        String anoF = conf.get("anoF");
        String agregacao = conf.get("agregacao");

        double sum = 0;
        int cont = 0;
        for (DoubleWritable value : values) {
            cont++;
            sum += value.get();
            aux.add(value.get());
        }

        double avg = Double.parseDouble(df.format((sum / cont)));

        Double var = 0.0;

        for (Double v : aux) {
            var += Math.pow((v - avg), 2);
            var = Double.parseDouble(df.format(var));

        }
        var = var / cont;

        context.write(key, new DoubleWritable(var));

    }

}
