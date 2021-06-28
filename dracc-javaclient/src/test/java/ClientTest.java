import com.lzp.dracc.javaclient.api.DraccClient;
import com.lzp.dracc.javaclient.exception.DraccException;
import com.lzp.dracc.javaclient.jdracc.JDracc;

/**
 * Description:
 *
 * @author: Lu ZePing
 * @date: 2019/7/15 12:26
 */
public class ClientTest {
    public static void main(String[] args) throws DraccException, InterruptedException {
        DraccClient draccClient = new JDracc(30000,"127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669");
        draccClient.addConfig("aaa","1");
        draccClient.addConfig("aaa","2");
        draccClient.addConfig("aaa","3");
        draccClient.addConfig("aaa","3");
        System.out.println(draccClient.getConfig("aaa"));
    }
}
