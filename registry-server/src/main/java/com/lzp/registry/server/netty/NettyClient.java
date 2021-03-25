package com.lzp.registry.server.netty;

import com.lzp.registry.common.constant.Cons;
import com.lzp.registry.common.zpproto.LzpMessageEncoder;
import com.lzp.registry.server.raft.RaftNode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Description:nettyclient
 *
 * @author: Lu ZePing
 * @date: 2020/9/27 18:32
 */
public class NettyClient implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);
    public static EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private static Bootstrap bootstrap = new Bootstrap();

    static {
        bootstrap.group(workerGroup).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
            @Override
            protected void initChannel(Channel channel) {
                channel.pipeline().addLast(new IdleStateHandler(15, Integer.MAX_VALUE, Integer.MAX_VALUE))
                        .addLast(new LzpRaftMessageDecoder(false)).addLast(new LzpMessageEncoder())
                        .addLast("resultHandler", new ResultHandler());
            }
        });
    }


    /**
     * Description:
     * 如果term没对方新，对方会返回一个消息，然后这边把term更新为最新,并且角色转换成follower
     * 如果index没对方新，对方不会投票
     * 如果对方当前任期已投过票也不会投票
     *
     * @author: Lu ZePing
     * @date: 2020/9/27 18:32
     */
    public static Channel getChannelAndRequestForVote(String ip, int port, long term, long index) {
        try {
            Channel channel = bootstrap.connect(ip, port).sync().channel();
            channel.writeAndFlush(RaftNode.getCommandId() + Cons.COMMAND_SEPARATOR + "reqforvote" + Cons
                    .COMMAND_SEPARATOR + term + Cons.COMMAND_SEPARATOR + index);
            return channel;
        } catch (Exception e) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                LOGGER.error(e.getMessage(), e);
            }
            if (RaftNode.getTerm() != term) {
                return null;
            } else if (Cons.LEADER.equals(RaftNode.getRole())) {
                return getChannelAndCheckSync(ip, port, term, index);
            } else if (Cons.CANDIDATE.equals(RaftNode.getRole())) {
                return getChannelAndRequestForVote(ip, port, term, index);
            }
        }
    }


    /**
     * Description:
     * 本节点已经被选举为主节点了,但是还有少数从节点处于失连状态
     * 则需要不断重连,并且在连接成功后发起日志同步请求(如果不一致的话)
     *
     * @author: Lu ZePing
     * @date: 2020/9/27 18:32
     */
    public static Channel getChannelAndCheckSync(String ip, int port, long term, long index) {
        try {
            Channel channel = bootstrap.connect(ip, port).sync().channel();
            channel.writeAndFlush(RaftNode.getCommandId() + Cons.COMMAND_SEPARATOR + "reqforsync" + Cons
                    .COMMAND_SEPARATOR + term + Cons.COMMAND_SEPARATOR + index);
            return channel;
        } catch (Exception e) {
            if (Cons.LEADER.equals(RaftNode.getRole())) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOGGER.error(e.getMessage(),e);
                }
                return getChannelAndCheckSync(ip, port, term, index);
            }
            return null;
        }
    }



    @Override
    public void close() {
        workerGroup.shutdownGracefully();
    }



}
