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
public class TopTen {

    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
	    // Stores a map of user reputation to the record
        
        private TreeMap<Integer,Text> repToRecordMap = new TreeMap<Integer,Text>();
	    
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = transformXmlToMap(value.toString());
	        String userId = parsed.get("AccountId");
            String reputation = parsed.get("Reputation");
            
            // Add this record to our map with the reputation as the key
            if(userId == null || reputation==null) return;
            repToRecordMap.put(Integer.parseInt(reputation),new Text(value));

            // If we have more than ten records, remove the one with the lowest reputation.
            if (repToRecordMap.size() > 10) {
                repToRecordMap.remove(repToRecordMap.firstKey());
            }
	    }

        protected void cleanup(Context context) throws IOException, InterruptedException {
        // Output our ten records to the reducers with a null key
            for (Text t : repToRecordMap.values()) {
                context.write(NullWritable.get(), t);
            }
        }   
	}
    

    public static class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        // Stores a map of user reputation to the record
        // Overloads the comparator to order the reputations in descending order
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                Map<String, String> parsed = transformXmlToMap(value.toString());
                String reputation = parsed.get("Reputation");
                repToRecordMap.put(Integer.parseInt(parsed.get("Reputation")),new Text(parsed.get("DisplayName")+"::"+parsed.get("Reputation")));

                // If we have more than ten records, remove the one with the lowest reputation
                if (repToRecordMap.size() > 10) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }  
            }

            for (Text t : repToRecordMap.descendingMap().values()) {
                // Output our ten records to the file system with a null key
                context.write(NullWritable.get(), t);
            }
        }
    }   

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

    /*public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(TopTen.class);
	conf.setJobName("topten");

	conf.setOutputKeyClass(NullWritable.class);
	conf.setOutputValueClass(Text.class);

	conf.setMapperClass(TopTenMapper.class);
	conf.setCombinerClass(TopTenReducer.class);
	conf.setReducerClass(TopTenReducer.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

    conf.setNumReduceTasks(1);
    
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
    }*/

        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopTen <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Top Ten Users by Reputation");
        job.setJarByClass(TopTen.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
