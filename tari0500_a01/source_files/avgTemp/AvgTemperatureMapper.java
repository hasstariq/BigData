package avgTemp;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgTemperatureMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15, 19);
        Integer month = Integer.parseInt(line.substring(19, 21));
        String time = line.substring(23, 27);
        int airTemperature;
        // parseInt doesn't like leading plus signs
        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        if(month >= 5 && month <= 8 && time.equals("0600")) {
            if (airTemperature != MISSING) {
                context.write(new Text(year), new IntWritable(airTemperature));
            }
        }
    }

}