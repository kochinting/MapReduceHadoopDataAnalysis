package kochinting.hw5;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
/**
 * Created by hdadmin on 10/22/16.
 */

public class MovieRatingsReducer extends Reducer<CompositeKeyWritable, Text, Text, Text> {

    Text movieID = new Text("");
    Text tab = new Text("\t");
    StringBuilder reduceValueBuilder = new StringBuilder("");
    Text reduceOutputValue = new Text("");
    String strSeparator = "\t";

    int count = 0;
    int ratingSUM = 0;
    float sum = 0;

    @Override
    protected void reduce(CompositeKeyWritable ckey, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        movieID.set(ckey.getjoinKey());
        for (Text value : values) {

            if (ckey.getsourceIndex() == 2) {
                reduceValueBuilder.append(value.toString()).append(strSeparator);
                reduceOutputValue.set(reduceValueBuilder.toString());
                context.write(movieID, reduceOutputValue);

                Counter ReducerCounter = context.getCounter(MyCounter.NUMBER_MOVIES);
                ReducerCounter.increment(1);

            } else if (ckey.getsourceIndex() == 1) {
                // movie rating data
                ratingSUM = Integer.parseInt(value.toString());
                sum += ratingSUM;
                count++;
            }
        }
        if (count != 0) {
            reduceValueBuilder.append("Average Rating: " + Math.round(sum / count)).append(strSeparator);
        }
        reduceValueBuilder.append("#of users: " + count).append(strSeparator).append("#of ratings: " + count).append(strSeparator);
        //reduceValueBuilder.append("SUM: "+sum).append(strSeparator);

        if (reduceValueBuilder.length() > 1 && ckey.getsourceIndex()==1) {
            //reduceValueBuilder.setLength(reduceValueBuilder.length() - 1);
            // Emit output
            reduceOutputValue.set(reduceValueBuilder.toString());
            //context.write(tab, reduceOutputValue);
            count = 0;
            sum = 0;
        } else {
            {
                System.out.println("Key=" + ckey.getjoinKey() + "src="
                        + ckey.getsourceIndex());
            }
            // Reset variables
            reduceValueBuilder.setLength(0);
            reduceOutputValue.set("");
        }
    }
}




