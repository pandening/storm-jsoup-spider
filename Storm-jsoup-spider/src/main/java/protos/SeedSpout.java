package protos;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by hujian on 2016/8/24.
 * this is the spout of the storm topology.
 * and this class just emit a seed site to next bolt,so easy~
 */
public class SeedSpout extends BaseRichSpout{
    //the tag of this class
    private final String TAG="SeedSpout";
    //the logger
    private  LogHandler logger=null;
    //the seed handler
    private  UtilsHandler Seeder=null;
    //the spout output collector
    private SpoutOutputCollector _collector;

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //the output file named "SEED"
        outputFieldsDeclarer.declare(new Fields("SEED"));
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        //get the output collector
        this._collector=spoutOutputCollector;
        //get the logger
        logger=new LogHandler();
        //setup the logger
        logger.setLogger(true);
        //get the seeder
        Seeder=UtilsHandler.getInstance();
        //test the logger...
        logger.info(this.TAG,"spout-open-setup the logger done~");
    }

    public void nextTuple() {
        //get a seed then emit the seed site to next bolt,then sleep 1 second
        String seed_site=UtilsHandler.get();
        if(seed_site==null){
            logger.debug(this.TAG,"the seeder is empty!");
            //sleep 5 seconds
            Utils.sleep(5000);
        }else{
            //send the seed site to next bolt
            this._collector.emit(new Values(seed_site));
            logger.info(this.TAG,"emit a seed site=>"+seed_site);
            Utils.sleep(1000);
        }
    }
}
