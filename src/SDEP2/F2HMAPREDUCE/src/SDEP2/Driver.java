package SDEP2;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.jfree.ui.RefineryUtilities;

/*This class is responsible for running map reduce job*/
public class Driver extends Configured implements Tool {

    public int runMR(String[] args) throws Exception {

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

        // Deleta a pasta output se ela existir
        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(args[6]), true);

        if (conf.get("agregacao").equals("semana")) {
            conf.set("cont", "0");
        }

        Job job = Job.getInstance(conf);

        job.setJarByClass(Driver.class);

        job.setJobName("Results para" + args[1]);

        int anoIni = 0;

        for (anoIni = Integer.parseInt(args[2]); anoIni <= Integer.parseInt(args[3]); anoIni++) {
            if (args[4].equals("media")) {
                MultipleInputs.addInputPath(job, new Path(args[0] + "/" + anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
            } else if (args[4].equals("max")) {
                MultipleInputs.addInputPath(job, new Path(args[0] + "/" + anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
            }
            if (args[4].equals("min")) {
                MultipleInputs.addInputPath(job, new Path(args[0] + "/" + anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
            }
            if (args[4].equals("var")) {
                MultipleInputs.addInputPath(job, new Path(args[0] + "/" + anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
            }
            if (args[4].equals("desvio")) {
                MultipleInputs.addInputPath(job, new Path(args[0] + "/" + anoIni), CombinedInputFormat.class, SimpleStatisticsMapper.class);
            }
            if (args[4].equals("mq")) {
                MultipleInputs.addInputPath(job, new Path(args[0] + "/" + anoIni), CombinedInputFormat.class, MinimosMapper.class);
            }
        }

        FileOutputFormat.setOutputPath(job, new Path(args[6]));

        switch (args[4]) {
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

        return (job.waitForCompletion(true) ? 0 : 1);

    }

    //Inicia o Map Reduce e após, cria o grafico
    public int run(String[] args) throws Exception {
        runMR(args);
        Dictionary dic = new Dictionary();
        if (args[4] != "mq") {
            Graphs chart = new Graphs(
                    "F2H GRAPH MAKER",
                    dic.translateOperacoes(args[4]) + ": " + dic.translateNomeDado(args[1]),
                    dic.translateAgregacao(args[5]), dic.translateNomeDado(args[1]), args[6]);
            chart.pack();
            RefineryUtilities.centerFrameOnScreen(chart);
            chart.setVisible(true);
            Runtime.getRuntime().exec(args[6]+"/normalChart.jpeg");
            
         }

        return 0;
    }

    public void start(String[] info) throws Exception {
        Driver driver = new Driver();
        int exitCode = ToolRunner.run(driver, info);
        System.exit(exitCode);

    }
}
