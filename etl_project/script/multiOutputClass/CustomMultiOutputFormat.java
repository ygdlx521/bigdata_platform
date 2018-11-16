package com.custom;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;

public class CustomMultiOutputFormat extends MultipleTextOutputFormat<Text, Text> {
    /**
     * Use they key as part of the path for the final output file.
     */
    @Override
    protected String generateFileNameForKeyValue(Text key, Text value, String leaf) {
        /*
        String val = value.toString();
        String[] values = val.split("\u0001");
        String ds = values[7];
        String d = "";
        String hh = "";
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
            Date date = sdf.parse(ds);
            SimpleDateFormat day = new SimpleDateFormat("yyyy-MM-dd");
            d = day.format(date);
            SimpleDateFormat hour = new SimpleDateFormat("HH");
            hh = hour.format(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        */
        //return "../../../" + key.toString() + "/event_day="+ d + "/event_hour=" + hh + "/"+ leaf;
        //return key.toString().split(":")[0] + "/event_day="+ key.toString().split(":")[1] + "/event_hour=" + key.toString().split(":")[2] + "/"+ leaf;
        return key.toString().split(":")[0];
    }

    /**
     * We discard the key as per your requirement
     */
    @Override
    protected Text generateActualKey(Text key, Text value) {
        return null;
    }
}
