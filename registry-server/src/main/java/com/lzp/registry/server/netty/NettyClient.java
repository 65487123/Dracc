package com.lzp.registry.server.netty;

import com.lzp.registry.common.constant.Const;
import com.lzp.registry.common.zpproto.LzpMessageEncoder;
import com.lzp.registry.server.raft.RaftNode;
import com.lzp.registry.server.raft.Role;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


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
    public static Channel getChannelAndRequestForVote(String requstId, String ip, int port, long term, long committedIndex, long uncommittedNum) {
        try {
            Channel channel = bootstrap.connect(ip, port).sync().channel();
            channel.writeAndFlush((requstId + Const.COMMAND_SEPARATOR + Const.RPC_ASKFORVOTE + Const.COMMAND_SEPARATOR + term + Const
                    .COMMAND_SEPARATOR + committedIndex + Const.COMMAND_SEPARATOR + uncommittedNum).getBytes(UTF_8));
            return channel;
        } catch (Exception e) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                LOGGER.error(e.getMessage(), e);
            }
            if (RaftNode.term != term) {
                return null;
            } else if (Role.LEADER == RaftNode.getRole()) {
                return getChannelAndAskForSync(ip, port, term);
            } else if (Role.CANDIDATE == RaftNode.getRole()) {
                return getChannelAndRequestForVote(requstId, ip, port, term, committedIndex, uncommittedNum);
            } else {
                return null;
            }
        }
    }


    /**
     * Description:
     * 本节点已经被选举为主节点了,但是还有少数从节点处于失连状态
     * 则需要不断重连,连接成功后发送一个同步请求(主要是同步任期)
     *
     * @author: Lu ZePing
     * @date: 2020/9/27 18:32
     */
    public static Channel getChannelAndAskForSync(String ip, int port, long term) {
        try {
            Channel channel = bootstrap.connect(ip, port).sync().channel();
            channel.writeAndFlush(("x" + Const.COMMAND_SEPARATOR + Const.RPC_SYNC_TERM +
                    Const.COMMAND_SEPARATOR + RaftNode.term).getBytes(UTF_8));
            return channel;
        } catch (Exception e) {
            if (Role.LEADER == RaftNode.getRole() && RaftNode.term == term) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    LOGGER.error(e.getMessage(), e);
                }
                return getChannelAndAskForSync(ip, port, term);
            }
            return null;
        }
    }


    @Override
    public void close() {
        workerGroup.shutdownGracefully();
    }



}
