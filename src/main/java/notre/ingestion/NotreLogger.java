package notre.ingestion;

import java.util.logging.Logger;
public class NotreLogger {
    Logger LOG;
    public NotreLogger(String className){
        LOG = Logger.getLogger(className);
    }
    public void info(Object message){
        LOG.info(message.toString());
    }
    public void warning(Object message){
        LOG.warning(message.toString());
    }

}
