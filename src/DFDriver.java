import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * DFDriver
 *
 * This driver configures and launches Job 2 of the TF-IDF pipeline.
 * Job 2 computes document frequency for each word.
 *
 * Input:
 *     output from Job 1 (document@word    tf)
 *
 * Output:
 *     word    df
 */
public class DFDriver {

    /*
     * main
     *
     * Launches the document-frequency job.
     *
     * args[0] - input path in HDFS (Job 1 output)
     * args[1] - output path in HDFS
     */
    public static void main(String[] args) throws Exception {

        // Validate command-line arguments.
        if (args.length != 2) {
            System.err.println("Usage: DFDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create Hadoop configuration and job instance.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Document Frequency Job");

        // Set the jar main class.
        job.setJarByClass(DFDriver.class);

        // Set mapper and reducer classes.
        job.setMapperClass(DFMapper.class);
        job.setReducerClass(DFReducer.class);

        // Set output key/value types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set input and output locations.
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exit with success or failure status.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}