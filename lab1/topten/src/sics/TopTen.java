package sics;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TopTen {

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {

        // Stores a map of user reputation to the record
        private TreeMap<Integer,Text> repToRecordMap = new TreeMap<Integer,Text>();

        //log handler for logging
        private static final Log log = LogFactory.getLog(TopTenMapper.class);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //uncomment this to see that value is an single xml record. 
            //log.info("======>"+value);

            Map<String, String> parsed = transformXmlToMap(value.toString());

            //retrieve relevant information from the map
            String displayName = parsed.get("DisplayName");
            String reputation = parsed.get("Reputation");
            if(displayName == null || "".equals(displayName.trim()) || reputation==null || "".equals(reputation.trim())) return;

            // Add this record to TreeMap using reputation as the key
            repToRecordMap.put(Integer.parseInt(reputation),new Text(displayName+"::"+reputation));

            // if the size of tree map becomes more than 10 records, delete the minimum key from map
            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            //output the key.value pair for reducers. Key is null.
            for (Text t : repToRecordMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }   
    }


    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
        public static final Log log = LogFactory.getLog(TopTenReducer.class);

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String displayName;
            int reputation;
            for (Text value : values) {

                String[] arr = value.toString().split("::");
                displayName = arr[0];
                try{
                    reputation = Integer.parseInt(arr[1]);
                }
                catch(NumberFormatException nfe){
                    continue;
                }	
                repToRecordMap.put(reputation,new Text(displayName+"::"+reputation));

                //if the size of tree map becomes more than 10 records, delete the minimum key from map
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }  
            }
            log.info("=====>Final Map Size:"+repToRecordMap.size());
            for (Text t : repToRecordMap.descendingMap().values()) {
                // Output our ten records to the file system with a null key
                context.write(NullWritable.get(), t);
            }
        }
    }   

    /*
     * Utility function to trasnform xml string into map.
     * Code taken from https://github.com/adamjshook/mapreducepatterns/blob/master/MRDP/src/main/java/mrdp/utils/MRDPUtils.java 
     */
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3)
                .split("\"");

            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];

                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "TopTen");
        job.setJarByClass(TopTen.class);

        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
