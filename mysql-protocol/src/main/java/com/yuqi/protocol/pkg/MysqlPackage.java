package com.yuqi.protocol.pkg;

import com.yuqi.engine.io.IO;
import com.yuqi.protocol.io.ReaderAndWriter;
import com.yuqi.protocol.utils.IOUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * @author yuqi
 * @mail yuqi4733@gmail.com
 * @description your description
 * @time 30/6/20 21:28
 **/
public class MysqlPackage implements ReaderAndWriter {
    /**
     * Length of Message body
     */
    private int lengthOfMessage;

    /**
     * Sequence number
     */
    private byte seqNumber;

    /**
     * Package contennt
     */
    private AbstractReaderAndWriter abstractReaderAndWriterPackage;


    public MysqlPackage() {
    }

    public MysqlPackage(AbstractReaderAndWriter abstractReaderAndWriterPackage) {
        this.abstractReaderAndWriterPackage = abstractReaderAndWriterPackage;
    }

    public int getLengthOfMessage() {
        return lengthOfMessage;
    }

    public byte getSeqNumber() {
        return seqNumber;
    }

    public AbstractReaderAndWriter getAbstractReaderAndWriterPackage() {
        return abstractReaderAndWriterPackage;
    }

    public void setLengthOfMessage(int lengthOfMessage) {
        this.lengthOfMessage = lengthOfMessage;
    }

    public void setSeqNumber(byte seqNumber) {
        this.seqNumber = seqNumber;
    }

    public void setAbstractReaderAndWriterPackage(AbstractReaderAndWriter abstractReaderAndWriterPackage) {
        this.abstractReaderAndWriterPackage = abstractReaderAndWriterPackage;
    }

    @Override
    public void read(ByteBuf byteBuf) {
        //read and then
        this.lengthOfMessage = IOUtils.readInteger(byteBuf, 3);
        this.seqNumber = IOUtils.readByte(byteBuf);
        abstractReaderAndWriterPackage.read(byteBuf);
    }

    @Override
    public void write(ByteBuf byteBuf) {

        ByteBuf tmp = PooledByteBufAllocator.DEFAULT.buffer(128);
        try {
            abstractReaderAndWriterPackage.write(tmp);

            this.lengthOfMessage = tmp.readableBytes();
            IOUtils.writeInteger3(lengthOfMessage, byteBuf);
            IOUtils.writeByte(seqNumber, byteBuf);
            byte[] bytes = new byte[tmp.readableBytes()];
            tmp.readBytes(bytes);

            //change from writeBytes -->  writeBytesWithoutEndFlag
            IOUtils.writeBytesWithoutEndFlag(bytes, byteBuf);
        } finally {
            IOUtils.decreaseReference(tmp);
        }

    }
}
