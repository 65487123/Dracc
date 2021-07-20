import com.lzp.dracc.javaclient.api.DraccClient;
import com.lzp.dracc.javaclient.exception.DraccException;
import com.lzp.dracc.javaclient.jdracc.JDracc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.time.LocalTime;
import java.util.concurrent.*;

/**
 * Description:
 *
 * @author: Lu ZePing
 * @date: 2019/7/15 12:26
 */
public class ClientTest {
    public static void main(String[] args) throws Exception {
        DraccClient draccClient = new JDracc(3000, "10.240.70.180:6669", "10.240.70.180:6668", "10.240.70.180:6667");

        //功能测试
        //config
        draccClient.addConfig("aaa", "1");
        draccClient.addConfig("aaa", "2");
        draccClient.addConfig("aaa", "3");
        draccClient.addConfig("aaa", "3");
        System.out.println(draccClient.getConfig("aaa"));
        draccClient.removeConfig("aaa", "3");
        System.out.println(draccClient.getConfig("aaa"));

        //service
        System.out.println(draccClient.registerInstance("serviceTest", "34.2.0.1", 8888));
        System.out.println(draccClient.registerInstance("serviceTest", "125.2.0.1", 8889));
        System.out.println(draccClient.registerInstance("serviceTest", InetAddress.getLocalHost().getHostAddress(), 8889));
        System.out.println(draccClient.getAllInstances("serviceTest"));
        System.out.println(draccClient.deregisterInstance("serviceTest", "34.2.0.1", 8888));
        System.out.println(draccClient.getAllInstances("serviceTest"));
        //事件监听
        draccClient.subscribe("serviceTest", instances -> System.out.println("监听到服务变动,变动后的服务实例列表为:" + instances));
        //检测服务健康检查
        Thread.sleep(2000);
        System.out.println(draccClient.getConfig("aaa"));
        //由于注册的服务实例"125.2.0.1:8889"都是乱写的,会被检测到不可达然后删除.
        System.out.println(draccClient.getAllInstances("serviceTest"));
        draccClient.unsubscribe("serviceTest");

        //lock
        //分布式锁测试应该用多台主机测试,我这里就开两个客户端模拟两台主机简单测试下
        ExecutorService threadPool = new ThreadPoolExecutor(2,2,0,TimeUnit.SECONDS,new LinkedBlockingQueue<>());
        threadPool.execute(() -> {
            DraccClient draccClient1 = null;
            try {
                draccClient1 = new JDracc(3000, "10.240.70.180:6669", "10.240.70.180:6668", "10.240.70.180:6667");
            } catch (Exception e) {
                e.printStackTrace();
            }
            draccClient1.acquireDistributedLock("testLock");
            System.out.println(Thread.currentThread().getName()+"成功拿到分布式锁:"+ LocalTime.now());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            draccClient1.releaseDistributedlock("testLock");
            System.out.println(Thread.currentThread().getName()+"释放分布式锁:"+ LocalTime.now());
        });
        threadPool.execute(() -> {
            DraccClient draccClient1 = null;
            try {
                draccClient1 = new JDracc(3000, "10.240.70.180:6669", "10.240.70.180:6668", "10.240.70.180:6667");
            } catch (Exception e) {
                e.printStackTrace();
            }
            draccClient1.acquireDistributedLock("testLock");
            System.out.println(Thread.currentThread().getName()+"成功拿到分布式锁:"+ LocalTime.now());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            draccClient1.releaseDistributedlock("testLock");
            System.out.println(Thread.currentThread().getName() + "释放分布式锁:" + LocalTime.now());
        });
        threadPool.shutdown();
        threadPool.awaitTermination(10000,TimeUnit.SECONDS);

        //性能测试
        //写配置性能
        //模拟开30个客户端并发写配置,每个客户端写1000条配置,测总共耗时(这里只是很简单地测试,但也可以看出大致的性能水平)
        System.out.println("开始性能测试");
        threadPool = new ThreadPoolExecutor(30, 30, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        long beginTime = System.currentTimeMillis();
        for (int i = 0; i < 30; i++) {
            threadPool.execute(() -> {
                String keyAndVal;
                for (int j = 0; j < 1000; j++) {
                    try {
                        draccClient.addConfig(keyAndVal = String.valueOf(j), keyAndVal);
                    } catch (DraccException ignored) {
                    }
                }
            });
        }
        threadPool.shutdown();
        threadPool.awaitTermination(10000, TimeUnit.SECONDS);
        long time = System.currentTimeMillis() - beginTime;
        System.out.println("30个客户端并发写配置,TPS = " + (new BigDecimal(30000).divide(new BigDecimal(time)
                .divide(new BigDecimal(1000), 3, RoundingMode.CEILING), 3, RoundingMode.CEILING)));
        //读配置性能
        threadPool = new ThreadPoolExecutor(30, 30, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        beginTime = System.currentTimeMillis();
        for (int i = 0; i < 30; i++) {
            threadPool.execute(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        draccClient.getConfig(String.valueOf(j));
                    } catch (DraccException ignored) {
                    }
                }
            });
        }
        threadPool.shutdown();
        threadPool.awaitTermination(10000, TimeUnit.SECONDS);
        time = System.currentTimeMillis() - beginTime;
        System.out.println("30个客户端并发读配置,QPS = " + (new BigDecimal(30000).divide(new BigDecimal(time)
                .divide(new BigDecimal(1000), 3, RoundingMode.CEILING), 3, RoundingMode.CEILING)));
    }
}
