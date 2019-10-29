import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MissingCard {
  private static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

    @SuppressWarnings("unused")
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //value = (HEART,2)
      //value = (Diamond,3)
      String line = value.toString();
      String[] lineSplit = line.split(",");
      context.write(new Text(lineSplit[0]), new IntWritable(Integer.parseInt(lineSplit[1])));
    }
  }

  private static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      // Input to Reducer = (HEART,(1,2,3,4,7,8,9,11,12));
      // Input to Reducer = (DIAMOND,(1,2,3,4,7,9,11,12));
      ArrayList<Integer> nums = new ArrayList<>();
      int sum = 0;
      int tempVal;
      for (IntWritable val : values) {
        sum+= val.get();
        tempVal = val.get();
        nums.add(tempVal);
      }

      if(sum < 91){
        for (int i = 1;i <= 13;i++){
          if(!nums.contains(i))
            context.write(key, new IntWritable(i));
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "missing card");
    job.setJarByClass(MissingCard.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
