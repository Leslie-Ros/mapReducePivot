import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PivotMapReduce {

  public static class MapperPivot
       extends Mapper<Object, Text, IntWritable, Text>{

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
              int index=0;
              IntWritable keyMap = new IntWritable();
              Text word = new Text();
              StringTokenizer itr = new StringTokenizer(value.toString(),",");
              while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                keyMap.set(index);
                context.write(keyMap, word);
                index += 1;
              }
            }
          }

  public static class ReducerPivot
       extends Reducer<IntWritable,Text,IntWritable,Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
              Text output = new Text();
              String ligne ="";
              for (Text val : values) {
                ligne = ligne + val.toString() + ",";
              }
              ligne = ligne.substring(0, ligne.length() - 1);
              output.set(ligne);
              context.write(key,output);
            }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "pivotMapReduce");
    job.setJarByClass(PivotMapReduce.class);
    job.setMapperClass(MapperPivot.class);
    job.setCombinerClass(ReducerPivot.class);
    job.setReducerClass(ReducerPivot.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
