package protos;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * Created by hujian on 2016/8/24.
 * this bolt is the No.2 bolt of this proto
 * in this bolt,the bolt will get the links from the filename->content
 * then add the seed site to the seedMap.
 * done!so easy~
 */
public class FindLinksBolt extends BaseRichBolt{
    //the tag of this class
    private final String TAG="FindLinksBolt";
    //the logger
    private  LogHandler logger=null;
    //the seed handler
    private  UtilsHandler Seeder=null;
    //the file name
    private String filename=null;
    //the output collector
    private OutputCollector _collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //get the collector
        this._collector=outputCollector;
        //get the logger
        logger=new LogHandler();
        Seeder=UtilsHandler.getInstance();
        //set up the logger
        logger.setLogger(true);
        //test the logger
        logger.info(this.TAG,"hello,i am No.2 bolt of this proto");
    }

    public void execute(Tuple tuple) {
        //get the file name
        filename=tuple.getString(0);
        //parse the file..
        File inputFile=new File(filename);
        Document doc= null;
        try {
            doc = Jsoup.parse(inputFile, "UTF-8","hujian");
        } catch (IOException e) {
            logger.debug(this.TAG,"fail to open the file=>"+filename);
            e.printStackTrace();
        }
        //find the links of this site...
        Elements links = doc.select("a[href]");
        for (Element link : links) {
            if(link.attr("abs:href").length()>0) {
                //add the sed
                UtilsHandler.set(link.attr("abs:href"));
                logger.info(this.TAG,"get a link:"+link.attr("abs:href"));
            }
        }
        //logger.info(this.TAG,"execute done!=>"+filename);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //ok,this is all,no next bolt of this bolt..
    }
}
