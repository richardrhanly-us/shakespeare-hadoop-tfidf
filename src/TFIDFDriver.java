import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * TFIDFDriver
 *
 * This driver launches Job 3 of the TF-IDF pipeline.
 * It reads:
 *
 * 1. TF output from Job 1
 * 2. DF output from Job 2
 *
 * Both are joined by word in the reducer to compute TF-IDF scores
 * and output the top 50 terms per document.
 */
public class TFIDFDriver {

    /*
     * main
     *
     * Launches the TF-IDF job using two separate input paths.
     *
     * args[0] - TF input path in HDFS
     * args[1] - DF input path in HDFS
     * args[2] - output path in HDFS
     */
    public static void main(String[] args) throws Exception {

        // Validate command-line arguments.
        if (args.length != 3) {
            System.err.println("Usage: TFIDFDriver <tf input path> <df input path> <output path>");
            System.exit(-1);
        }

        // Create Hadoop configuration and job instance.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TF-IDF Top 50 Terms Job");

        // Set the jar main class.
        job.setJarByClass(TFIDFDriver.class);

        // Use multiple inputs so TF and DF outputs can both feed into this job.
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TFIDFMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TFIDFMapper.class);

        // Set reducer class.
        job.setReducerClass(TFIDFReducer.class);

        // Use one reducer so the final top-50 logic is centralized.
        job.setNumReduceTasks(1);

        // Set output key/value classes.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set final output path.
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Exit with success or failure status.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}