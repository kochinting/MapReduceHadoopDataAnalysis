package kochinting.hw5;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.Logger;

/**
 * Created by hdadmin on 10/22/16.
 */

public class MovieRatingsMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, Text>{

    Logger logger = Logger.getLogger(MovieRatingsMapper.class);
    //IntWritable one = new IntWritable(1);
    Text txtValue = new Text("");
    int intSrcIndex=0;
    StringBuilder strMapValueBuilder = new StringBuilder("");
    List<Integer> lstRequiredAttribList = new ArrayList<Integer>();
    CompositeKeyWritable cKey = new CompositeKeyWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout"+ context.getTaskAttemptID().toString() + " " +  fileName);
        System.err.println ("in stderr"+ context.getTaskAttemptID().toString());

        FileSplit fsFileSplit = (FileSplit) context.getInputSplit();



        if (fsFileSplit.getPath().getName().equals("u1.data")
                || fsFileSplit.getPath().getName().equals("u2.data")
                || fsFileSplit.getPath().getName().equals("u3.data")
                || fsFileSplit.getPath().getName().equals("u4.data")
                || fsFileSplit.getPath().getName().equals("u5.data")){
            intSrcIndex =1;
        }
        if (fsFileSplit.getPath().getName().equals("u.item") ){
            intSrcIndex =2;
        }
        // Initialize the list of fields to emit as output based on
        // intSrcIndex (1: u.data, 2=u.item)
        if (intSrcIndex == 1 ) // movie-rating
        {
            //lstRequiredAttribList.add(1); // movie id
            lstRequiredAttribList.add(2); // movie rating
            //lstRequiredAttribList.add(3); // movie rating

        } else if (intSrcIndex == 2 ) // movie information
        {
            lstRequiredAttribList.add(1); // Title
            lstRequiredAttribList.add(2); // release date
            lstRequiredAttribList.add(4); // URL
        }
    }

    private String buildMapValue(String arrEntityAttributesList[]) {

        strMapValueBuilder.setLength(0);// Initialize
        // Build list of attributes to output based on source
        for (int i = 1; i < arrEntityAttributesList.length; i++) {
            // If the field is in the list of required output
            // append to stringbuilder
            if (lstRequiredAttribList.contains(i)) {
                strMapValueBuilder.append(arrEntityAttributesList[i]).append("\t");
            }
        }
        if (strMapValueBuilder.length() > 0) {
            // Drop last comma
            strMapValueBuilder.setLength(strMapValueBuilder.length() - 1);
        }
        return strMapValueBuilder.toString();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value.toString().length() > 0 && intSrcIndex==1) {
            String arrEntityAttributes[] = value.toString().split("\t");
            cKey.setjoinKey(arrEntityAttributes[1]);
            cKey.setsourceIndex(intSrcIndex);
            txtValue.set(buildMapValue(arrEntityAttributes));
            context.write(cKey, txtValue);
            Counter MapperCounter = context.getCounter(MyCounter.TOTAL_RECORDS);
            MapperCounter.increment(1);
        }
        if (value.toString().length() > 0 && intSrcIndex==2) {
            String arrEntityAttributes[] = value.toString().split("\\|");
            cKey.setjoinKey(arrEntityAttributes[0]);
            cKey.setsourceIndex(intSrcIndex);
            txtValue.set(buildMapValue(arrEntityAttributes));
            context.write(cKey, txtValue);
        }
    }
}
