
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class TestWriteV4 {
    // 生成路径
    private static final String path = "TreeTsFileV4.tsfile";
    // 非对齐设备名
    private static String nonAlignedDeviceName = "root.db.v4.d1";


    /**
     * 生成tsfile文件
     */
    public static void main(String[] args) {
        try {
            File f = FSFactoryProducer.getFSFactory().getFile(path);
            // 判断文件是否存在
            if (f.exists()) {
                Files.delete(f.toPath());
            }
            // 创建 TsFileWriter 对象
            TsFileWriter tsFileWriter = new TsFileWriter(f);
            // 执行写入次数
            write_tsfile(tsFileWriter);
            // 关闭写入
            tsFileWriter.close();
            System.out.println("文件生成成功");
        } catch (IOException | WriteProcessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 写入非对齐数据
     */
    private static void write_tsfile(TsFileWriter tsFileWriter) throws WriteProcessException, IOException {
        // 注册 schema
        List<IMeasurementSchema> schemasNonAligned = new ArrayList<>(10);
        schemasNonAligned.add(new MeasurementSchema("m_INT32_", TSDataType.INT32));
        schemasNonAligned.add(new MeasurementSchema("m_TEXT_", TSDataType.TEXT));
        schemasNonAligned.add(new MeasurementSchema("m_STRING_", TSDataType.STRING));
        schemasNonAligned.add(new MeasurementSchema("m_BLOB_", TSDataType.BLOB));
        for (IMeasurementSchema schema : schemasNonAligned) {
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, schema);
        }

        // 生成 tablet
        Tablet tablet = new Tablet(nonAlignedDeviceName, schemasNonAligned, 10);
        tablet.setRowSize(10);
        long time = 100L;
        for (int row = 0; row < tablet.getRowSize(); row++) {
            tablet.addTimestamp(row, time++);
            tablet.addValue("m_INT32_", row, 1);
            tablet.addValue("m_TEXT_", row, "text");
            tablet.addValue("m_STRING_", row, "string");
            tablet.addValue("m_BLOB_", row, new Binary("string".getBytes(StandardCharsets.UTF_8)));
        }
        // 写入 tablet
        tsFileWriter.writeTree(tablet);
    }

}
