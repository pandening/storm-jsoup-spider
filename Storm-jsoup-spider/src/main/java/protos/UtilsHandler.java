package protos;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hujian on 2016/8/23.
 * in this class,you will set/get the work dir,and get/put the file.
 * 1、you can get a seed by calling getSeed
 * 2、you can append a seed by calling putSeed
 * 3、you can use setDir/getDir to set/get the work dir
 * 4、dummy by calling the dummy
 * 5、you need to set the start seed!or use the default seed "http://www.hao123.com"
 */
public class UtilsHandler {
    //this is the only instance of this class.
    private static UtilsHandler _instance=null;
    //the work dir
    private static String WorkDir="/work";
    //the seed
    private static String seedSite="http://www.hao123.com";
    //the current link seed map.
    Map<String,Integer> CurrentSeedMap;
    //the get seed link map
    Map<String,Integer> SeedMap;

    //upset,you can not invoke this constructor...
    private UtilsHandler(){
        CurrentSeedMap=new HashMap<String, Integer>();
        SeedMap=new HashMap<String, Integer>();
        //this is the seed site...
        CurrentSeedMap.put(seedSite,0);
    }
    //so,this is the only way to get the instance of this class.
    public static UtilsHandler getInstance(){
        if(_instance==null){
            _instance=new UtilsHandler();
        }
        System.out.println("[UtilsHandler] private constructor ok~");
        return _instance;
    }
    //set the dir
    public static void setWorkDir(String dir){
        WorkDir=dir;
    }
    //get the dir
    public static String getWorkDir(){
        return WorkDir;
    }
    //get a seed
    public static String get(){
     if(_instance!=null){
         if(!_instance.CurrentSeedMap.isEmpty()){
             System.out.println("[UtilsHandler]the current now size is:"+_instance.CurrentSeedMap.size());
             System.out.println("[UtilsHandler]the SeedMap now size is:" + _instance.SeedMap.size());
             String seed_site = _instance.CurrentSeedMap.entrySet().iterator().next().getKey();
             _instance.CurrentSeedMap.remove(seed_site);
             return seed_site;
         }else{
             if(!_instance.SeedMap.isEmpty()){
                 System.out.println("[UtilsHandler]SeedMap to Current...:");
                 _instance.CurrentSeedMap.putAll(_instance.SeedMap);
                 _instance.SeedMap.clear();
                 String seed_site_ = _instance.CurrentSeedMap.keySet().iterator().next();
                 _instance.CurrentSeedMap.remove(seed_site_);
                 System.out.println("[UtilsHandler]the current now size is:"+_instance.CurrentSeedMap.size());
                 System.out.println("[UtilsHandler]the SeedMap now size is:" + _instance.SeedMap.size());
                 return seed_site_;
             }else{
                 System.out.println("[UtilsHandler]All Empty!!!!!!");
                 return null;
             }
         }
     }else{
         System.out.println("[UtilsHandler]oh,the _instance is empty...");
         return null;
     }
    }
    //set key
    public static void set(String site){
        if(_instance!=null){
            _instance.SeedMap.put(site,0);
        }
    }
    //dummy website to file
    public static void dummy(BufferedReader reader,String path){
        //i just need the file reader and path,then,i will dummy the content
        //to this path file
        try {
            BufferedWriter writer=new BufferedWriter(new FileWriter(path));
            String line="";
            while((line=reader.readLine())!=null){
                writer.append(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //as debug,you can use this function to show all seed now~
    public void AllSeeds(){
        System.out.println("[UtilsHandler]the current seed map");
        for(Map.Entry<String,Integer>sd:_instance.CurrentSeedMap.entrySet()){
            System.out.println("<current>:"+sd.getKey());
        }
        System.out.println("[UtilsHandler]the seedMap");
        for(Map.Entry<String,Integer>sd:_instance.SeedMap.entrySet()){
            System.out.println("<SeedMap>"+sd.getKey());
        }
    }
    //you maybe need get a file name random
    //i will get a uuid from system,then return the uuid+".xxx"
    //so,please offer the ".xxx",or use default ".txt"
    public static String getAFileName(String type){
        if(type==null||type.length()==0){
            type=".txt";
        }
        return java.util.UUID.randomUUID().toString()+type;
    }
}
