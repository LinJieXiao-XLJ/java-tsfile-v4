package org.apache.iotdb.tree;

import org.apache.iotdb.utils.ReadConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;

/**
 * 树模型V4版本：同一个device, 相同时间分区
 */
public class TreeTsFileV4_SameDevice_SameTimeStamp {
    // 实例化配置
    private static final ReadConfig config = ReadConfig.getInstance();
    // 生成路径
    private static final String path = config.getConfigValue("POSITION") + "TreeTsFileV4_SameDevice_SameTimeStamp.tsfile";
    // 目标文件
    private final File f = FSFactoryProducer.getFSFactory().getFile(path);
    // 数据库名
    private static String databaseName = "root." + config.getConfigValue("DATABASE_NAME");
    // 非对齐设备名前缀
    private static String nonAlignedDeviceName = databaseName + "." + config.getConfigValue("NON_ALIGNED_DEVICE_NAME");
    // 对齐设备名前缀
    private static String alignedDeviceName = databaseName + "." + config.getConfigValue("ALIGNED_DEVICE_NAME");
    // 物理量名前缀
    private static final String measurementName = config.getConfigValue("MEASUREMENT_NAME_TREE");
    // 存放非对齐序列的 schema
    private final List<IMeasurementSchema> schemasNonAligned = new ArrayList<>(10);
    // 存放对齐序列的 schema
    private final List<IMeasurementSchema> schemasAligned = new ArrayList<>(10);

    /**
     * 生成tsfile文件
     */
    public void testWrite() {
        // 根据配置判断是否需要添加后缀
        if (config.getConfigValue("IS_UNIQUE_DATABASE_NAME").equals("true")) {
            databaseName = databaseName + "_NoSameDevice_NoSameTimeStamp";
        }
        if (config.getConfigValue("IS_UNIQUE_DEVICE_NAME").equals("true")) {
            nonAlignedDeviceName = nonAlignedDeviceName + "_NoSameDevice_NoSameTimeStamp";
            alignedDeviceName = alignedDeviceName + "_NoSameDevice_NoSameTimeStamp";
        }
        try {
            // 判断文件是否存在
            if (f.exists()) {
                Files.delete(f.toPath());
            }
            // 创建 TsFileWriter 对象
            TsFileWriter tsFileWriter = new TsFileWriter(f);
            for (int times = 0; times < Integer.parseInt(config.getConfigValue("LOOP")); times++) {
                // 写入非对齐
                write_tsfile(tsFileWriter, times);
            }
            // 写入对齐
            writer_tsfile_align(tsFileWriter);
            // 关闭写入
            tsFileWriter.close();
        } catch (IOException | WriteProcessException e) {
            System.out.println(e.getMessage());
        }
    }

    /**
     * 写入非对齐数据
     */
    private void write_tsfile(TsFileWriter tsFileWriter, int times) throws WriteProcessException, IOException {
        for (int i = 0; i < Integer.parseInt(config.getConfigValue("NON_ALIGNED_SCHEMAS_NUMBER")); i++) {
            MeasurementSchema measurementSchema1 = new MeasurementSchema(measurementName + "_" + "BOOLEAN" + "_" + times + "_" + i, TSDataType.BOOLEAN);
            schemasNonAligned.add(measurementSchema1);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema1);
            MeasurementSchema measurementSchema2 = new MeasurementSchema(measurementName + "_" + "INT32" + "_" + times + "_" + i, TSDataType.INT32);
            schemasNonAligned.add(measurementSchema2);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema2);
            MeasurementSchema measurementSchema3 = new MeasurementSchema(measurementName + "_" + "INT64" + "_" + times + "_" + i, TSDataType.INT64);
            schemasNonAligned.add(measurementSchema3);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema3);
            MeasurementSchema measurementSchema4 = new MeasurementSchema(measurementName + "_" + "FLOAT" + "_" + times + "_" + i, TSDataType.FLOAT);
            schemasNonAligned.add(measurementSchema4);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema4);
            MeasurementSchema measurementSchema5 = new MeasurementSchema(measurementName + "_" + "DOUBLE" + "_" + times + "_" + i, TSDataType.DOUBLE);
            schemasNonAligned.add(measurementSchema5);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema5);
            MeasurementSchema measurementSchema6 = new MeasurementSchema(measurementName + "_" + "TEXT" + "_" + times + "_" + i, TSDataType.TEXT);
            schemasNonAligned.add(measurementSchema6);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema6);
            MeasurementSchema measurementSchema7 = new MeasurementSchema(measurementName + "_" + "STRING" + "_" + times + "_" + i, TSDataType.STRING);
            schemasNonAligned.add(measurementSchema7);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema7);
            MeasurementSchema measurementSchema8 = new MeasurementSchema(measurementName + "_" + "BLOB" + "_" + times + "_" + i, TSDataType.BLOB);
            schemasNonAligned.add(measurementSchema8);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema8);
            MeasurementSchema measurementSchema9 = new MeasurementSchema(measurementName + "_" + "TIMESTAMP" + "_" + times + "_" + i, TSDataType.TIMESTAMP);
            schemasNonAligned.add(measurementSchema9);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema9);
            MeasurementSchema measurementSchema10 = new MeasurementSchema(measurementName + "_" + "DATE" + "_" + times + "_" + i, TSDataType.DATE);
            schemasNonAligned.add(measurementSchema10);
            tsFileWriter.registerTimeseries(nonAlignedDeviceName, measurementSchema10);

            Tablet tablet = new Tablet(nonAlignedDeviceName, schemasNonAligned, Integer.parseInt(config.getConfigValue("ROW_NUMBER")));
            tablet.setRowSize(Integer.parseInt(config.getConfigValue("ROW_NUMBER")));
            long time = Long.parseLong(config.getConfigValue("SAME_PARTITION_TIMESTAMP"));
            for (int row = 0; row < tablet.getRowSize(); row++) {
                tablet.addTimestamp(row, time++);
                if (config.getConfigValue("IS_CONTAIN_NULL_VALUES").equals("false")) {
                    tablet.addValue(measurementName + "_" + "BOOLEAN" + "_" + times + "_" + i, row, getRandom().nextBoolean());
                    tablet.addValue(measurementName + "_" + "INT32" + "_" + times + "_" + i, row, getRandom().nextInt(-2147483647, 2147483647));
                    tablet.addValue(measurementName + "_" + "INT64" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                    tablet.addValue(measurementName + "_" + "FLOAT" + "_" + times + "_" + i, row, (float) getRandom().nextDouble(-2147483647, 2147483647));
                    tablet.addValue(measurementName + "_" + "DOUBLE" + "_" + times + "_" + i, row, getRandom().nextDouble(-2147483647, 2147483647));
                    tablet.addValue(measurementName + "_" + "TEXT" + "_" + times + "_" + i, row, getString(1000));
                    tablet.addValue(measurementName + "_" + "STRING" + "_" + times + "_" + i, row, getString(100));
                    tablet.addValue(measurementName + "_" + "BLOB" + "_" + times + "_" + i, row, new Binary(getString(100).getBytes(StandardCharsets.UTF_8)));
                    tablet.addValue(measurementName + "_" + "TIMESTAMP" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                    tablet.addValue(measurementName + "_" + "DATE" + "_" + times + "_" + i, row, LocalDate.ofEpochDay(getRandom().nextInt(-100000, 100000)));
                } else {
                    if (row % 2 == 0) {
                        tablet.addValue(measurementName + "_" + "BOOLEAN" + "_" + times + "_" + i, row, getRandom().nextBoolean());
                        tablet.addValue(measurementName + "_" + "INT32" + "_" + times + "_" + i, row, getRandom().nextInt(-2147483647, 2147483647));
                        tablet.addValue(measurementName + "_" + "INT64" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                        tablet.addValue(measurementName + "_" + "FLOAT" + "_" + times + "_" + i, row, (float) getRandom().nextDouble(-2147483647, 2147483647));
                        tablet.addValue(measurementName + "_" + "DOUBLE" + "_" + times + "_" + i, row, getRandom().nextDouble(-2147483647, 2147483647));
                    } else {
                        tablet.addValue(measurementName + "_" + "TEXT" + "_" + times + "_" + i, row, getString(1000));
                        tablet.addValue(measurementName + "_" + "STRING" + "_" + times + "_" + i, row, getString(100));
                        tablet.addValue(measurementName + "_" + "BLOB" + "_" + times + "_" + i, row, new Binary(getString(100).getBytes(StandardCharsets.UTF_8)));
                        tablet.addValue(measurementName + "_" + "TIMESTAMP" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                        tablet.addValue(measurementName + "_" + "DATE" + "_" + times + "_" + i, row, LocalDate.ofEpochDay(getRandom().nextInt(-1000, 1000)));
                    }
                }
                tsFileWriter.flush();
            }
            tsFileWriter.writeTree(tablet);
            schemasNonAligned.clear();
        }
    }

    /**
     * 写入对齐数据
     */
    private void writer_tsfile_align(TsFileWriter tsFileWriter) throws WriteProcessException, IOException {
        // 注册 schema
        for (int times = 0; times < Integer.parseInt(config.getConfigValue("LOOP")); times++) {
            for (int i = 0; i < Integer.parseInt(config.getConfigValue("ALIGNED_SCHEMAS_NUMBER")); i++) {
                MeasurementSchema measurementSchema1 = new MeasurementSchema(measurementName + "_" + "BOOLEAN" + "_" + times + "_" + i, TSDataType.BOOLEAN);
                schemasAligned.add(measurementSchema1);
                MeasurementSchema measurementSchema2 = new MeasurementSchema(measurementName + "_" + "INT32" + "_" + times + "_" + i, TSDataType.INT32);
                schemasAligned.add(measurementSchema2);
                MeasurementSchema measurementSchema3 = new MeasurementSchema(measurementName + "_" + "INT64" + "_" + times + "_" + i, TSDataType.INT64);
                schemasAligned.add(measurementSchema3);
                MeasurementSchema measurementSchema4 = new MeasurementSchema(measurementName + "_" + "FLOAT" + "_" + times + "_" + i, TSDataType.FLOAT);
                schemasAligned.add(measurementSchema4);
                MeasurementSchema measurementSchema5 = new MeasurementSchema(measurementName + "_" + "DOUBLE" + "_" + times + "_" + i, TSDataType.DOUBLE);
                schemasAligned.add(measurementSchema5);
                MeasurementSchema measurementSchema6 = new MeasurementSchema(measurementName + "_" + "TEXT" + "_" + times + "_" + i, TSDataType.TEXT);
                schemasAligned.add(measurementSchema6);
                MeasurementSchema measurementSchema7 = new MeasurementSchema(measurementName + "_" + "STRING" + "_" + times + "_" + i, TSDataType.STRING);
                schemasAligned.add(measurementSchema7);
                MeasurementSchema measurementSchema8 = new MeasurementSchema(measurementName + "_" + "BLOB" + "_" + times + "_" + i, TSDataType.BLOB);
                schemasAligned.add(measurementSchema8);
                MeasurementSchema measurementSchema9 = new MeasurementSchema(measurementName + "_" + "TIMESTAMP" + "_" + times + "_" + i, TSDataType.TIMESTAMP);
                schemasAligned.add(measurementSchema9);
                MeasurementSchema measurementSchema10 = new MeasurementSchema(measurementName + "_" + "DATE" + "_" + times + "_" + i, TSDataType.DATE);
                schemasAligned.add(measurementSchema10);
            }
        }
        tsFileWriter.registerAlignedTimeseries(alignedDeviceName, schemasAligned);

        long time = Long.parseLong(config.getConfigValue("SAME_PARTITION_TIMESTAMP"));
        for (int times = 0; times < Integer.parseInt(config.getConfigValue("LOOP")); times++) {
            // 生成 tablet
            Tablet tablet = new Tablet(alignedDeviceName, schemasAligned, Integer.parseInt(config.getConfigValue("ROW_NUMBER")));
            tablet.setRowSize(Integer.parseInt(config.getConfigValue("ROW_NUMBER")));
            for (int row = 0; row < tablet.getRowSize(); row++) {
                tablet.addTimestamp(row, time++);
                for (int i = 0; i < Integer.parseInt(config.getConfigValue("ALIGNED_SCHEMAS_NUMBER")); i++) {
                    if (config.getConfigValue("IS_CONTAIN_NULL_VALUES").equals("false")) {
                        tablet.addValue(measurementName + "_" + "BOOLEAN" + "_" + times + "_" + i, row, getRandom().nextBoolean());
                        tablet.addValue(measurementName + "_" + "INT32" + "_" + times + "_" + i, row, getRandom().nextInt(-2147483647, 2147483647));
                        tablet.addValue(measurementName + "_" + "INT64" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                        tablet.addValue(measurementName + "_" + "FLOAT" + "_" + times + "_" + i, row, (float) getRandom().nextDouble(-2147483647, 2147483647));
                        tablet.addValue(measurementName + "_" + "DOUBLE" + "_" + times + "_" + i, row, getRandom().nextDouble(-2147483647, 2147483647));
                        tablet.addValue(measurementName + "_" + "TEXT" + "_" + times + "_" + i, row, getString(1000));
                        tablet.addValue(measurementName + "_" + "STRING" + "_" + times + "_" + i, row, getString(100));
                        tablet.addValue(measurementName + "_" + "BLOB" + "_" + times + "_" + i, row, new Binary(getString(100).getBytes(StandardCharsets.UTF_8)));
                        tablet.addValue(measurementName + "_" + "TIMESTAMP" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                        tablet.addValue(measurementName + "_" + "DATE" + "_" + times + "_" + i, row, LocalDate.ofEpochDay(getRandom().nextInt(-100000, 100000)));
                    } else {
                        if (row % 2 == 0) {
                            tablet.addValue(measurementName + "_" + "BOOLEAN" + "_" + times + "_" + i, row, getRandom().nextBoolean());
                            tablet.addValue(measurementName + "_" + "INT32" + "_" + times + "_" + i, row, getRandom().nextInt(-2147483647, 2147483647));
                            tablet.addValue(measurementName + "_" + "INT64" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                            tablet.addValue(measurementName + "_" + "FLOAT" + "_" + times + "_" + i, row, (float) getRandom().nextDouble(-2147483647, 2147483647));
                            tablet.addValue(measurementName + "_" + "DOUBLE" + "_" + times + "_" + i, row, getRandom().nextDouble(-2147483647, 2147483647));
                        } else {
                            tablet.addValue(measurementName + "_" + "TEXT" + "_" + times + "_" + i, row, getString(1000));
                            tablet.addValue(measurementName + "_" + "STRING" + "_" + times + "_" + i, row, getString(100));
                            tablet.addValue(measurementName + "_" + "BLOB" + "_" + times + "_" + i, row, new Binary(getString(100).getBytes(StandardCharsets.UTF_8)));
                            tablet.addValue(measurementName + "_" + "TIMESTAMP" + "_" + times + "_" + i, row, getRandom().nextLong(-9223372036854775807L, 9223372036854775807L));
                            tablet.addValue(measurementName + "_" + "DATE" + "_" + times + "_" + i, row, LocalDate.ofEpochDay(getRandom().nextInt(-1000, 1000)));
                        }
                    }
                    tsFileWriter.flush();
                }
            }
            // 写入 tablet
            tsFileWriter.writeTree(tablet);
            // 清理环境
            schemasAligned.clear();
        }
    }

    /**
     * 用于生成随机数值
     */
    private SplittableRandom getRandom() {
        return new SplittableRandom();
    }

    /**
     * 用于生成随机字符串
     */
    private String getString(int length) {
        final String CHAR_SET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +  // 大写字母
                "abcdefghijklmnopqrstuvwxyz" +                  // 小写字母
                "0123456789"                                    // 数字
                + "!@#$%^&*()-_=+[]{}|;:,.<>?/~`" +             // 符号
                "去微软推哦爬山的风格结合两者相差不能";                // 中文字符
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(CHAR_SET.length());
            sb.append(CHAR_SET.charAt(randomIndex));
        }
        return sb.toString();
    }

    /**
     * 读取数据
     */
    public void testReader() {
        // 查询非对齐时间序列
        try(TsFileReader reader = new TsFileReader(f)) {
            List<Path> selectedSeries = new ArrayList<>();
            selectedSeries.add(new Path(nonAlignedDeviceName, measurementName + "_" + "INT32" + "_0_0", true));
            selectedSeries.add(new Path(nonAlignedDeviceName, measurementName + "_" + "TIMESTAMP" + "_0_0", true));
            QueryDataSet dataSet = reader.query(QueryExpression.create(selectedSeries, null));
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 查询对齐时间序列
        try(TsFileReader reader = new TsFileReader(f)) {
            List<Path> selectedSeries = new ArrayList<>();
            selectedSeries.add(new Path(alignedDeviceName, measurementName + "_" + "INT32" + "_0_0", true));
            selectedSeries.add(new Path(alignedDeviceName, measurementName + "_" + "TIMESTAMP" + "_0_0", true));
            QueryDataSet dataSet = reader.query(QueryExpression.create(selectedSeries, null));
            while (dataSet.hasNext()) {
                System.out.println(dataSet.next());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}