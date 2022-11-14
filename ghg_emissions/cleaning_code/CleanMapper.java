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
            String header = row[0].trim().replaceAll(" ", "_") + ',' + row[2].trim().replaceAll(" ", "_") + ",";
            String[] years = {"2001", "2002", "2003", "2004", "2005", "2006", "2007", "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019"}; 
            
            // Format data to be in iso, sector, year, emissions 
            for (int i = 0; i < 19; i++) {
                String output =  header + years[i] + "," + row[i + 15]; 
                context.write(new Text(output), new IntWritable(1)); 
            }
        }
    }
}



