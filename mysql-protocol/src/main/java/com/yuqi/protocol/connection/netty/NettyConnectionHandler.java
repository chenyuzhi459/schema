package com.yuqi.protocol.connection.netty;

import com.google.common.collect.Maps;
import com.yuqi.protocol.pkg.auth.ServerGreeting;
import com.yuqi.protocol.utils.IOUtils;
import com.yuqi.protocol.utils.PackageUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.Map;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 30/6/20 21:06
 **/
public class NettyConnectionHandler extends ChannelInboundHandlerAdapter {

    public static final NettyConnectionHandler INSTANCE = new NettyConnectionHandler();

    private Map<Channel, ConnectionContext> alreadyAuthenChannels = Maps.newConcurrentMap();

    public Map<Channel, ConnectionContext> getAlreadyAuthenChannels() {
        return alreadyAuthenChannels;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        //todo, do some log
        Channel channel = ctx.channel();
        sendAuthencationPackage(channel);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
       getAlreadyAuthenChannels().remove(ctx.channel());
    }

    private boolean sendAuthencationPackage(Channel channel) {
        ServerGreeting serverGreetingPackage = PackageUtils.buildInitAuthencatinPackage();
        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.buffer(128);
        try {
            serverGreetingPackage.write(tmp);

            ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(128);
            IOUtils.writeInteger3(tmp.readableBytes(), buf);
            IOUtils.writeByte((byte) 0x00, buf);

            buf.writeBytes(tmp);

            channel.writeAndFlush(buf);

            return true;
        } finally {
            IOUtils.decreaseReference(tmp);
        }

    }

    public boolean channelHasAuthencation(Channel channel) {
        return alreadyAuthenChannels.containsKey(channel);
    }


}
