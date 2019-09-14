import org.apache.log4j.Logger;

/**
 * log4j模拟日志的产生
 */
public class LogGenerator {
    private static Logger logger =Logger.getLogger(LogGenerator.class.getName());
    public static void main(String[] args) throws Exception{
       int index=0;
        while (true) {
            Thread.sleep(1000);
            logger.info("value "+ index++ );
        }
            }
}
