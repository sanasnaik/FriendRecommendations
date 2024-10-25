// Sana Naik
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendRecommendations extends Configured implements Tool {
   
   // this will parse the file and assign friend/user/mutual groups
   public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

      @Override
      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // for each line, parse the user and their list of friends
        String[] line = value.toString().split("\t");
        if (line.length != 2) return;
        String user = line[0];
        String[] friends = line[1].split(",");

        // then for each friend, add a tuple between the user and their friend + vice versa.
        for (int i = 0; i < friends.length; i++) {
            context.write(new Text(user), new Text(friends[i]));
            context.write(new Text(friends[i]), new Text(user));

            // for each friend of the user, add the user as their mutual friend with every other friend in the list. 
            // needed to create friend recommendations - keep track of how many mutual friends each pair has.
            for (int j = i + 1; j < friends.length; j++) {
                context.write(new Text(friends[i]), new Text("M" + friends[j]));
                context.write(new Text(friends[j]), new Text("M" + friends[i]));
            }
        }
      }
   }

   // go through pairs with mutual friends, eliminate people who are already friends, provide top 10 recommendations
   public static class FriendReducer extends Reducer<Text, Text, Text, Text> {

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

         HashMap<String, Integer> mutualsCount = new HashMap<>();
         Set<String> alreadyFriends = new HashSet<>();

         // go through each user we have written with a friend in common
         for (Text val: values) {
            String person = val.toString();
            if (person.startsWith("M")) {  // if they have a friend in common
               person = person.substring(1); 
               if (!alreadyFriends.contains(person)) {
                  mutualsCount.put(person, mutualsCount.getOrDefault(person, 0) + 1);  // add 1 to the count of mutuals
               }
            }
            else { 
               alreadyFriends.add(person);  // add to the alreadyFriends set
            }
         }
         
         // sort the recommendations of friends numerically ascending order
         int len = Math.min(mutualsCount.size(), 10);
         String[] recommendations = new String[len];
         for (int i = 0; i < recommendations.length; i++) {  // get top 10 recommendations
            String max = Collections.max(mutualsCount.entrySet(), Map.Entry.comparingByValue()).getKey();
            mutualsCount.remove(max);
            recommendations[i] = max;
         }
         
         context.write(key, new Text(String.join(",", recommendations)));
      }
   }

   public static void main(String[] args) throws IOException, InterruptedException, Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new FriendRecommendations(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
      System.out.println(Arrays.toString(args));
      Job job = Job.getInstance(getConf(), "FriendRecommendations");
      job.setJarByClass(FriendRecommendations.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(FriendMapper.class);
      job.setReducerClass(FriendReducer.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   } 
}
