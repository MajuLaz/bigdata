package advanced.customwritable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class TDE {
    public static void main(String[] args)throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // Os caminhos de entrada e saida
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j =  new Job(c, "TDE");

        j.setJarByClass(TDE.class);
        j.setMapperClass(TDEMap.class);
        j.setReducerClass(TDEReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //Setar os meus arquivos de entrada e saída do job
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class TDEMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String [] campos = linha.split(";");

            String ano = campos[1];
            String flowType = campos[4];
            if (ano.matches("\\d+")) {
                con.write(new Text(flowType + "," + ano + "."), new IntWritable(1));
            }
        }
    }

    public static class TDEReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;

            for(IntWritable v : values){
                soma += v.get();
            }

            con.write(key, new IntWritable(soma));
        }
    }
}
