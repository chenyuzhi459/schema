package com.yuqi.protocol.command;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.yuqi.protocol.connection.netty.ConnectionContext;
import com.yuqi.protocol.constants.ColumnTypeConstants;
import com.yuqi.protocol.pkg.MysqlPackage;
import com.yuqi.protocol.pkg.ResultSetHolder;
import com.yuqi.protocol.pkg.response.ColumnType;
import com.yuqi.protocol.pkg.response.EofPackage;
import com.yuqi.protocol.utils.IOUtils;
import com.yuqi.protocol.utils.PackageUtils;
import com.yuqi.sql.SlothColumn;
import com.yuqi.sql.SlothSchema;
import com.yuqi.sql.SlothSchemaHolder;
import com.yuqi.sql.SlothTable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.yuqi.protocol.constants.ErrorCodeAndMessageEnum.UNKNOWN_DB_NAME;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 26/7/20 17:18
 **/
public class FieldListCommandHandler extends AbstractCommandHandler {
    private String command;

    public FieldListCommandHandler(ConnectionContext connectionContext, ByteBuf byteBuf) {
        super(connectionContext);
        this.command = IOUtils.readString(byteBuf);
    }

    @Override
    public void execute() {

        final SlothSchema slothSchema = SlothSchemaHolder.INSTANCE.getSlothSchema(connectionContext.getDb());
        SlothTable table = (SlothTable)slothSchema.getTable(command);
        if(table == null){
            return;
        }
        List<SlothColumn> columns = table.getColumns();

        //query seqNumber is 0 so result query number from 1
        byte seqNumber = 1;
        final List<MysqlPackage> result = Lists.newArrayList();
        for (SlothColumn column : columns) {
            String columnName = column.getColumnName();
            int mysqlType = ColumnTypeConstants.getMysqlType(column.getColumnType().getColumnType());
            final MysqlPackage columnTypeMysqlPackage = new MysqlPackage();
            final ColumnType columnTypePackage =
                    ColumnType.builder()
                            .catalog("def")
                            .schema(slothSchema.getSchemaName())
                            .table(table.getTableName())
                            .orgTable(table.getTableName())
                            .name(columnName)
                            .originalName(columnName)
                            //original 33
                            .charSet(33)
                            .filler((byte) 0x0c)
                            //original 84
                            .columnLength(255)
                            .columnType((byte)mysqlType)
                            .flags(0x00)
                            .dicimals((byte) 0x00)
//                            .defaultValue(Strings.nullToEmpty(column.getColumnType().getDefalutValue()))
                            .build();

            columnTypeMysqlPackage.setAbstractReaderAndWriterPackage(columnTypePackage);
            columnTypeMysqlPackage.setSeqNumber(seqNumber++);
            result.add(columnTypeMysqlPackage);

        }

        //third is end of package
        final MysqlPackage eofPackage = new MysqlPackage(new EofPackage((byte) 0xfe, 0, 0x0002));
        eofPackage.setSeqNumber(seqNumber++);
        result.add(eofPackage);
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer(128);
        result.forEach(a -> a.write(buf));
        connectionContext.write(buf);
    }
}
