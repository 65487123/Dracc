import com.lzp.dracc.javaclient.EventListener;
import com.lzp.dracc.javaclient.api.DraccClient;
import com.lzp.dracc.javaclient.jdracc.JDracc;

import java.util.List;

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
        System.out.println(draccClient.registerInstance("serviceTest","34.2.0.1",8888));
        System.out.println(draccClient.registerInstance("serviceTest","123.2.0.1",8889));
        System.out.println(draccClient.registerInstance("serviceTest","125.2.0.1",8889));
        System.out.println(draccClient.getAllInstances("serviceTest"));
        System.out.println(draccClient.deregisterInstance("serviceTest","34.2.0.1",8888));
        System.out.println(draccClient.getAllInstances("serviceTest"));
        //事件监听
        draccClient.subscribe("serviceTest", var1 -> System.out.println("监听到服务变动,变动后的服务实例列表为:"+var1));
        //检测服务健康检查
        Thread.sleep(30000);
        System.out.println(draccClient.getConfig("aaa"));
        //由于注册的服务实例ip是乱写的,会被检测到不可达然后删除.
        System.out.println(draccClient.getAllInstances("serviceTest"));

        //lock


    }
}
