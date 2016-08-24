package protos;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by hujian on 2016/8/24.
 * main function.set up the storm
 */
public class Runner {
    //the tag
    private static final String TAG = "Runner";
    //this is the storm topology builder
    private static TopologyBuilder builder = new TopologyBuilder();
    //the log
    private static LogHandler logger = new LogHandler();

    public static void main(String[] args) {
        logger.setLogger(true);
        logger.info(TAG, "start to build the storm topology...");
        //storm config
        Config config = new Config();
        //set the spout
        builder.setSpout("SeedSpout", new SeedSpout(), 10);
        //set the no.1 bolt,get the seed from the spout
        builder.setBolt("ParseSeedSiteBolt", new ParseSeedSiteBolt(), 10)
                .shuffleGrouping("SeedSpout");
        //set the no.2 bolt
        builder.setBolt("FindLinksBolt", new FindLinksBolt(), 10)
                .shuffleGrouping("ParseSeedSiteBolt");
        //shut down the debug log
        config.setDebug(false);
        //choose the run mode.
        if (args != null && args.length > 0) {
            //remote
            config.setNumWorkers(2);
            try {
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {//local
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Counter", config, builder.createTopology());
            logger.info(TAG,"start to run local mode...");
            /*
            * if you want to shut down the cluster after run some time,just use
            * the follow codes.
            * */
            //Utils.sleep(100000);
            //cluster.killTopology("Counter");
            //cluster.shutdown();
        }
    }
}

