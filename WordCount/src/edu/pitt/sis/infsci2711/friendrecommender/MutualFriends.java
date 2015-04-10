package edu.pitt.sis.infsci2711.friendrecommender;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFriends extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FriendCountWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}
	
	public static class FriendCountWritable implements Writable {
	    public Long user;
	    public Long mutualFriend;

	    public FriendCountWritable(Long user, Long mutualFriend) {
	        this.user = user;
	        this.mutualFriend = mutualFriend;
	    }

	    public FriendCountWritable() {
	        this(-1L, -1L);
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeLong(user);
	        out.writeLong(mutualFriend);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	        user = in.readLong();
	        mutualFriend = in.readLong();
	    }

	    @Override
	    public String toString() {
	        return " toUser: "
	                + Long.toString(user) + " mutualFriend: "
	                + Long.toString(mutualFriend);
	    }
	}
	
	public static class Map extends Mapper<LongWritable, Text, LongWritable, FriendCountWritable> {
	    //private Text word = new Text();

	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	//scan a line from the input file and split it to the user id and his friends
	        String line[] = value.toString().split("\t");
	        //first element is the fromUser
	        Long fromUser = Long.parseLong(line[0]);
	        //remaining elements are the toUsers
	        List<Long> toUsers = new ArrayList<Long>();

	        //check if the user has friends
	        //if yes then extract all his friends as elements and add him to the array of toUsers
	        if (line.length == 2) {
	            StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
	            while (tokenizer.hasMoreTokens()) {
	                Long toUser = Long.parseLong(tokenizer.nextToken());
	                toUsers.add(toUser);
	              //setting the mutual friends between fromUser and toUser as -1 because they are already friends
	              //so no need to recommend them to each other as friends
	                context.write(new LongWritable(fromUser),
	                        new FriendCountWritable(toUser, -1L));
	            }

	            //since 2 toUsers are friends of fromUser
	            //the 2 toUsers have a mutual friend fromUser
	            //do this for all pairs of toUsers of fromUser
	            for (int i = 0; i < toUsers.size(); i++) {
	                for (int j = i + 1; j < toUsers.size(); j++) {
	                    context.write(new LongWritable((long) toUsers.get(i)),
	                            new FriendCountWritable((Long) (toUsers.get(j)), fromUser));
	                    context.write(new LongWritable((long) toUsers.get(j)),
	                            new FriendCountWritable((Long) (toUsers.get(i)), fromUser));
	                }
	            }
	        }
	    }
	}
	
	public static class Reduce extends Reducer<LongWritable, FriendCountWritable, LongWritable, Text> {
	    @Override
	    public void reduce(LongWritable key, Iterable<FriendCountWritable> values, Context context)
	            throws IOException, InterruptedException {

	        // key is the recommended friend, and value is the list of mutual friends
	        final java.util.Map<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();

	        for (FriendCountWritable val : values) {
	        	//if users are friends, mutualFriend has been set as -1 in map stage
	            final Boolean isAlreadyFriend = (val.mutualFriend == -1);
	            final Long toUser = val.user;
	            final Long mutualFriend = val.mutualFriend;

	            //check if the toUser has been used to find mutualFriends
	            if (mutualFriends.containsKey(toUser)) {
		            //check if the users are already friends
	                if (isAlreadyFriend) {
	                    mutualFriends.put(toUser, null);
	                }
	                //else add the mutualFriend in the list of mutualFriends
	                else if (mutualFriends.get(toUser) != null) {
	                    mutualFriends.get(toUser).add(mutualFriend);
	                }
	            }
	            //toUser has not been used yet to find mutualFriends
	            //add toUser as key to mutualFriends map
	            else {
	            	//if fromUser and toUser are not friends, add mutualFriend
	                if (!isAlreadyFriend) {
	                    mutualFriends.put(toUser, new ArrayList<Long>() {
	                        /**
							 * 
							 */
							private static final long serialVersionUID = 1L;

							{
	                            add(mutualFriend);
	                        }
	                    });
	                }
	                //else null for users that are already friends
	                else {
	                    mutualFriends.put(toUser, null);
	                }
	            }
	        }

	        //Sort the list according to the number of mutual friends
	        java.util.SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
				@Override
				public int compare(Long key1, Long key2) {
					/*Long key1 = (Long) o1;
					Long key2 = (Long) o2;*/
					Integer v1 = mutualFriends.get(key1).size();
	                Integer v2 = mutualFriends.get(key2).size();
	                //Sorted according to the descending order of number of mutual friends
	                if (v1 > v2) {
	                    return -1;
	                }
	                //Else sorted according to ascending order of userId if the number of mutual friends is same
	                else if (v1.equals(v2) && key1 < key2) {
	                    return -1;
	                } else {
	                    return 1;
	                }
				}
	        });

	        //add all the mutualFriends map entries in sortedMutualFriends map for
	        //sorting based on the number of mutual friends
	        for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) {
	            if (entry.getValue() != null) {
	                sortedMutualFriends.put(entry.getKey(), entry.getValue());
	            }
	        }

	        //display all the users and 10 recommended friends and number of mutual friends
	        Integer i = 0;
	        String output = "";
	        for (java.util.Map.Entry<Long, List<Long>> entry : sortedMutualFriends.entrySet()) {
	            if (i == 0) {
	                output = entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")";
	            } else if(i<10){
	                output += "," + entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")";
	            }
	            ++i;
	        }
	        context.write(key, new Text(output));
	    }
	}
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(), new MutualFriends(), args);
		
		System.exit(res);
	}

}
