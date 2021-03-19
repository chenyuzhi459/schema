package com.yuqi.protocol.connection.netty;

import com.yuqi.protocol.command.*;
import com.yuqi.protocol.pkg.MysqlPackage;
import com.yuqi.protocol.pkg.request.Command;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import static com.yuqi.protocol.constants.CommandTypeConstants.*;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 30/6/20 21:11
 **/
public class MysqlPackageHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        final MysqlPackage mySQLPackage = (MysqlPackage) msg;
        final ConnectionContext connectionContext = NettyConnectionHandler.INSTANCE
                .getAlreadyAuthenChannels().get(ctx.channel());

        dispatchCommandPackage((Command) mySQLPackage.getAbstractReaderAndWriterPackage(), connectionContext);
    }

    private void dispatchCommandPackage(Command commandPackage, ConnectionContext connectionContext) {
        final byte type = commandPackage.getCommandType();
        CommandHandler handler;
        switch (type) {
            case COM_PING:
            case COM_QUIT:
                handler = new EmptyCommandHandler(connectionContext);
                break;
            case COM_QUERY:
                handler = new QueryCommandHandler(connectionContext, commandPackage.getCommand());
                break;
            case COM_USE_DB:
                handler = new UseDatabaseCommandHandler(connectionContext, commandPackage.getCommand());
                break;
            case COM_FIELD_LIST:
                handler = new FieldListCommandHandler(connectionContext, commandPackage.getCommand());
                break;
            default:
                //todo
                handler = new QueryCommandHandler(connectionContext, commandPackage.getCommand());
        }

        handler.execute();
    }
}
