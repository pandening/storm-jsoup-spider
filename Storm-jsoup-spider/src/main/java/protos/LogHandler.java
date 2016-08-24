package protos;

/**
 * Created by hujian on 2016/8/24.
 * ok,just a log handler,you can set true to setup the logger
 * or set false to shut down the logger
 */
public class LogHandler {
    private boolean isSetup=false;
    //set the logger
    void setLogger(boolean is){
        this.isSetup=is;
    }
    //info
    void info(String cls,String msg){
        if(this.isSetup) {
            System.out.println("[Info_" + cls + "]" + msg);
        }
    }
    //debug
    void debug(String cls,String msg){
        if(this.isSetup){
            System.out.println("[Debug_"+cls+"]"+msg);
        }
    }
}
