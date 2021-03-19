package com.yuqi.protocol.command;

import com.yuqi.protocol.connection.netty.ConnectionContext;
import com.yuqi.protocol.pkg.MysqlPackage;
import com.yuqi.protocol.utils.PackageUtils;
import io.netty.buffer.ByteBuf;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 13/7/20 23:36
 **/
public class EmptyCommandHandler implements CommandHandler {
    protected ConnectionContext connectionContext;

    public EmptyCommandHandler(ConnectionContext connectionContext) {
        this.connectionContext = connectionContext;
    }

    @Override
    public void execute() {
        MysqlPackage mysqlPackage = PackageUtils.buildOkMySqlPackage(0, 1, 0);
        ByteBuf byteBuf = PackageUtils.packageToBuf(mysqlPackage);
        connectionContext.write(byteBuf);
    }
}
