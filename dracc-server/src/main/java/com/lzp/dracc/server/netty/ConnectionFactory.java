package com.lzp.dracc.server.netty;

import com.lzp.dracc.common.constant.Const;
import com.lzp.dracc.common.zpproto.LzpMessageEncoder;
import com.lzp.dracc.server.raft.RaftNode;
import com.lzp.dracc.server.raft.Role;
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
public class ConnectionFactory implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionFactory.class);
    public static EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    private static Bootstrap bootstrap = new Bootstrap();

    static {
        bootstrap.group(workerGroup).channel(NioSocketChannel.class).handler(new ChannelInitializer() {
            @Override
            protected void initChannel(Channel channel) {
                channel.pipeline().addLast(new IdleStateHandler(12, Integer.MAX_VALUE, Integer.MAX_VALUE))
                        .addLast(new LzpRaftMessageDecoder(false)).addLast(new LzpMessageEncoder())
                        .addLast("resultHandler", new ResultHandler());
            }
        });
    }


    /**
     * Description:
     * 建立连接,然后发送请求投票的消息,并且返回这个连接
     * 如果term没对方新，对方会返回一个消息，然后这边把term更新为最新,并且角色转换成follower
     * 如果index没对方新，对方不会投票
     * 如果对方当前任期已投过票也不会投票
     *
     * @author: Lu ZePing
     * @date: 2020/9/27 18:32
     */
    public static Channel newChannelAndRequestForVote(String requstId, String ip, int port, long term, long committedIndex, long uncommittedNum) {
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
                return newChannelAndAskForSync(ip, port, term);
            } else if (Role.CANDIDATE == RaftNode.getRole()) {
                return newChannelAndRequestForVote(requstId, ip, port, term, committedIndex, uncommittedNum);
            } else {
                return null;
            }
        }
    }


    /**
     * Description:
     * 建立连接,然后发送请求同步的消息,并且返回这个连接
     * 本节点已经被选举为主节点了,但是还有少数从节点处于失连状态
     * 则需要不断重连,连接成功后发送一个同步请求(主要是同步任期)
     *
     * @author: Lu ZePing
     * @date: 2020/9/27 18:32
     */
    public static Channel newChannelAndAskForSync(String ip, int port, long term) {
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
                return newChannelAndAskForSync(ip, port, term);
            }
            return null;
        }
    }


    @Override
    public void close() {
        workerGroup.shutdownGracefully();
    }



}
