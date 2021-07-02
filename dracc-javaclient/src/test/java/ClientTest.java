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
    public static void main(String[] args) throws Exception {
        DraccClient draccClient = new JDracc(3000,"127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669");
        //config
        draccClient.addConfig("aaa","1");
        draccClient.addConfig("aaa","2");
        draccClient.addConfig("aaa","3");
        draccClient.addConfig("aaa","3");
        System.out.println(draccClient.getConfig("aaa"));
        draccClient.removeConfig("aaa","3");
        System.out.println(draccClient.getConfig("aaa"));

        //service
        draccClient.registerInstance("serviceTest","127.0.0.1",8888);
        draccClient.registerInstance("serviceTest","127.0.0.1",8889);
        System.out.println(draccClient.getAllInstances("serviceTest"));
        //检测服务健康检查
        draccClient.close();
        Thread.sleep(30000);
        draccClient = new JDracc(3000,"127.0.0.1:6667", "127.0.0.1:6668", "127.0.0.1:6669");
        System.out.println(draccClient.getConfig("aaa"));
        System.out.println(draccClient.getAllInstances("serviceTest"));

    }
}
