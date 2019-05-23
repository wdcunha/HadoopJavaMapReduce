package map.reduce;

import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxDuration implements Writable {
    // declare variables
    Integer minDuration;
    Integer maxDuration;
    // constructor
    public MinMaxDuration() {
        minDuration=0;
        maxDuration=0;
    }
    //set method
    void setMinDuration(Integer duration){
        this.minDuration=duration;
    }
    void setMaxDuration(Integer duration){
        this.maxDuration=duration;
    }
    //get method
    Integer getMinDuration() {
        return minDuration;
    }
    Integer getMaxDuration(){
        return maxDuration;
    }

    // write method
    public void write(DataOutput out) throws IOException {
        // what order we want to write !
        out.writeInt(minDuration);
        out.writeInt(maxDuration);
    }

    // readFields Method
    public void readFields(DataInput in) throws IOException {
        minDuration=new Integer(in.readInt());
        maxDuration=new Integer(in.readInt());
    }

    public String toString() {
        return minDuration + "\t" + maxDuration;
    }

}