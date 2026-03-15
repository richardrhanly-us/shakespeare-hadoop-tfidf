import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * TFDriver
 *
 * This driver configures and launches Job 1 of the TF-IDF pipeline.
 * Job 1 computes term frequency for each word in each document.
 *
 * Input:
 *     folder containing text files
 *
 * Output:
 *     document@word    tf
 */
public class TFDriver {

    /*
     * main
     *
     * Launches the term-frequency job.
     *
     * args[0] - input path in HDFS
     * args[1] - output path in HDFS
     */
    public static void main(String[] args) throws Exception {

        // Validate command-line arguments.
        if (args.length != 2) {
            System.err.println("Usage: TFDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create Hadoop configuration and job instance.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Term Frequency Job");

        // Set the main class stored in the jar.
        job.setJarByClass(TFDriver.class);

        // Set mapper and reducer classes.
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);

        // Set output key/value types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit with success or failure status.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}