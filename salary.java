package salary;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	if (key.get() ==0){
    		return;
    	}
      
        String[] columns = value.toString().split(",");
        if (columns.length >= 8) {
            int salaryInUSD = Integer.parseInt(columns[7]);
            context.write(new Text("TotalSalary"), new IntWritable(salaryInUSD));
            context.write(new Text("JobTitle"), new IntWritable(1));
            context.write(new Text("ExperienceLevel_" + columns[2]), new IntWritable(salaryInUSD));
        }
    }
}




package salary;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalaryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable value : values) {
            int intValue = value.get();
            sum += intValue;
            count++;
        }

        if (key.toString().startsWith("ExperienceLevel_")) {
            // Calculate average salary for each experience level
            int averageSalary = sum / count;
            context.write(new Text(key.toString().replace("ExperienceLevel_", "") + "_AverageSalary"),
                    new IntWritable(averageSalary));
        } else if (key.toString().equals("JobTitle")) {
            // Count distinct job titles
            context.write(new Text("DistinctJobTitles"), new IntWritable(count));
        } else if (key.toString().equals("TotalSalary")) {
            // Calculate average of total salary_in_usd
            int averageTotalSalary = sum / count;
            context.write(new Text("AverageTotalSalary"), new IntWritable(averageTotalSalary));
        }
    }
}



package salary;

import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SalaryDriver extends Configured implements Tool {
    public int run(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Please give valid inputs");
            return -1;
        }
        Job job = Job.getInstance(getConf());
        job.setJobName("SalaryJob");

        job.setMapperClass(SalaryMapper.class);
        job.setReducerClass(SalaryReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        int exitCode = ToolRunner.run(new SalaryDriver(), args);
        System.out.println(exitCode);
    }
}
