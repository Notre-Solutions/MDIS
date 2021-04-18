package notre.ingestion;

import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.checkerframework.checker.units.qual.C;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class ChronicleQueueLauncher {
    NotreLogger LOG = new NotreLogger(ChronicleQueueLauncher.class.getName());
    String rootPath = "C:\\Users\\Nyasha\\IdeaProjects\\MDIS\\src\\main\\resources\\chronicle-queues";
    public static void main(String[] args) throws IOException {

        ChronicleQueueLauncher launcher = new ChronicleQueueLauncher();


        Chronicle chronicle = launcher.buildIndexedChronicleQueue();
        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = chronicle.createTailer();

        launcher.writeToQueues(appender);
        launcher.readQueues(tailer);


    }

    public Chronicle buildIndexedChronicleQueue() throws IOException {
        File queueDir = new File(getDirectory());
        if (! queueDir.exists()){
            queueDir.mkdirs();
            // If you require it to make the entire directory path including parents,
            // use directory.mkdirs(); here instead.
        }
        queueDir = new File(getDirectory("test"));
        Chronicle chronicle = ChronicleQueueBuilder.indexed(queueDir).build();
        return chronicle;

    }


    public String getDirectory(String chronicleFileName){
        StringBuilder sb = new StringBuilder(rootPath);
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDateTime now = LocalDateTime.now();
        return String.format("%s\\%s\\%s",rootPath,dtf.format(now),chronicleFileName);
    }

    public String getDirectory(){
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
        LocalDateTime now = LocalDateTime.now();
        return String.format("%s\\%s",rootPath,dtf.format(now));

    }

    public void writeToQueues(ExcerptAppender appender){
        appender.startExcerpt();
        String stringVal = "Hello World";
        int intVal = 101;
        long longVal = System.currentTimeMillis();
        double doubleVal = 90.00192091d;

        appender.writeUTF(stringVal);
        appender.writeInt(intVal);
        appender.writeLong(longVal);
        appender.writeDouble(doubleVal);
        appender.finish();
    }

    public void readQueues(ExcerptTailer tailer){
        while (tailer.nextIndex()) {
            String a = tailer.readUTF();
            int b = tailer.readInt();
            Long c = tailer.readLong();
            Double d = tailer.readDouble();
            LOG.info(a+" "+b+" "+c+" "+d);
        }

        tailer.finish();
    }
}
