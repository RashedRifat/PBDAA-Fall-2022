import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountRecsReducer extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
     @Override public void reduce( Text num,  Iterable<IntWritable> counts,  Context context)
         throws IOException,  InterruptedException {

    int sum=0;
      for ( IntWritable count  : counts) {
        sum  += count.get();
      }
      context.write(num,  new IntWritable(sum));
    }
} 
