//********************************************************************************
//Class:    CompositeKeyWritable
//Purpose:  Custom Writable that serves as composite key
//          with attributes joinKey and sourceIndex
//Reference:   http://hadooped.blogspot.com/2013/09/reduce-side-joins-in-java-map-reduce.html
//*********************************************************************************

package kochinting.hw5;

/**
 * Created by hdadmin on 10/22/16.
 */


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,
        WritableComparable<CompositeKeyWritable> {

    // Data members
    private String joinKey;// MovieID
    private int sourceIndex;// 1=Movie Rating; 2=Movie information

    public CompositeKeyWritable() {
    }

    public CompositeKeyWritable(String joinKey, int sourceIndex) {
        this.joinKey = joinKey;
        this.sourceIndex = sourceIndex;
    }

    @Override
    public String toString() {

        return (new StringBuilder().append(joinKey).append("\t")
                .append(sourceIndex)).toString();
    }

    public void readFields(DataInput dataInput) throws IOException {
        joinKey = WritableUtils.readString(dataInput);
        sourceIndex = WritableUtils.readVInt(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput, joinKey);
        WritableUtils.writeVInt(dataOutput, sourceIndex);
    }

    public int compareTo(CompositeKeyWritable objKeyPair) {

        int result = joinKey.compareTo(objKeyPair.joinKey);
        if (0 == result) {
            result = Double.compare(sourceIndex, objKeyPair.sourceIndex);
        }
        return result;
    }

    public String getjoinKey() {
        return joinKey;
    }

    public void setjoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public int getsourceIndex() {
        return sourceIndex;
    }

    public void setsourceIndex(int sourceIndex) {
        this.sourceIndex = sourceIndex;
    }
}