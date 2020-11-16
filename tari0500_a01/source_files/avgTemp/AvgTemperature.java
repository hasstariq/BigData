package avgTemp;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * /usr/local/hadoop/bin/hadoop fs -mkdir -p /user/hdfs/data/input/temperature
 *
 * /usr/local/hadoop/bin/hadoop fs -put 1904.gz /user/hdfs/data/input/temperature
 * /usr/local/hadoop/bin/hadoop fs -put 1905.gz /user/hdfs/data/input/temperature
 *
 * /usr/local/hadoop/bin/hadoop fs -chmod -R 777 /user/hdfs/data/input/temperature
 *
 * /usr/local/hadoop/bin/hadoop jar mapreduce-basic-tasks.jar avgTemp.AvgTemperature /user/hdfs/data/input/temperature /user/hdfs/data/output/temperature
 */
public class AvgTemperature {


  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: AvgTemperature <input path> <output path>");
      System.exit(-1);
    }

    Job job = new Job();
    job.setJarByClass(AvgTemperature.class);
    job.setJobName("Average temperature");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(AvgTemperatureMapper.class);
    job.setReducerClass(AvgTemperatureReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
