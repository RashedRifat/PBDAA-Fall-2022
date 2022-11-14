import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().toLowerCase(); 
        String[] row = line.split((",")); 
        
        if (row[0].contains("country")) return; // skip header row 
        if (!row[3].contains("all ghg")) return; // skip rows that do not contain all GHG

        // only process the rows that we need 
        if (row[2].contains("total including lucf") || (row[2].contains("land-use change and forestry"))) {
            String output = row[0].trim().replaceAll(" ", "_") + ',' + row[2].trim().replaceAll(" ", "_") + ",";
            output += String.join(",", Arrays.copyOfRange(row, 14, 34));  
            context.write(new Text(output), new IntWritable(1)); 
        }
    }
}