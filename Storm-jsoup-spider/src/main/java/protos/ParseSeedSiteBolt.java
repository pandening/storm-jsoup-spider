package protos;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by hujian on 2016/8/24.
 * this is the No.1 bolt in this project,the bolt will
 * get the seed site from the spout,then in this class,the bolt
 * will get the page content string,then store the content
 * finally,send the filename to next bolt~
 * the next bolt will get the content and get the information from
 * the content file.
 * the file name :get the timestamp,then set the name as the filename
 * or,get a UUID,let the file name equal as the uuid
 */
public class ParseSeedSiteBolt extends BaseRichBolt{
    //the tag of this class
    private final String TAG="ParseSeedSiteBolt";
    //the logger
    private  LogHandler logger=null;
    //the file name
    private String filename=null;
    //the seed handler
    private  UtilsHandler Seeder=null;
    //the output collector
    private OutputCollector _collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //get the collector
        this._collector=outputCollector;
        //get the seeder
        Seeder=UtilsHandler.getInstance();
        //get the logger
        logger=new LogHandler();
        //set up the logger
        logger.setLogger(true);
        //get a file name
        filename=UtilsHandler.getAFileName(".html");
        //test the logger and the filename!=null
        logger.info(this.TAG,"filename="+filename);
    }
    public void execute(Tuple tuple) {
        //get the seed site from tuple,then get the content of this seed
        String seed=tuple.getString(0);
        //if you want to look up the links,just use the following code..
        Seeder.AllSeeds();
        logger.info(this.TAG,"get the seed from spout=>"+seed);
        //ok,let get the content of this seed
        try {
            Document doc= Jsoup.connect(seed).get();
            //let output the doc to file
            BufferedWriter fileWriter=new BufferedWriter(new FileWriter(filename));
            if(fileWriter==null){
                logger.debug(this.TAG,"fail to create the file:"+filename);
            }else{
                //start to flush the content to file
                logger.info(this.TAG,"create file ok,start to flush to file...");
                fileWriter.append(doc.toString());
                fileWriter.close();
                logger.info(this.TAG,"flush a file done!");
                //emit the file name to next bolt
                this._collector.emit(new Values(filename));
                logger.info(this.TAG,"emit a file name to next bolt=>"+filename);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //i will emit the file name to next bolt
        outputFieldsDeclarer.declare(new Fields("FILE_NAME"));
    }
}
