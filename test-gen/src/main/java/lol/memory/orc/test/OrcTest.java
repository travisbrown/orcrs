package lol.memory.orc.test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public abstract class OrcTest implements Closeable {
    private final Path path;
    private final String schema;
    private final CompressionKind compression;
    private final Writer writer;
    private final VectorizedRowBatch batch;
    private int row;

    public static void main(String[] args) throws IOException {
        Path outputDir = new Path(args[0]);

        SimpleOrcTest simple1 = new SimpleOrcTest(outputDir, false);
        simple1.gen();

        SimpleOrcTest simple2 = new SimpleOrcTest(outputDir, true);
        simple2.gen();

        LongOrcTest long1 = new LongOrcTest(outputDir, false);
        long1.gen();

        LongOrcTest long2 = new LongOrcTest(outputDir, true);
        long2.gen();

        StringOrcTest string1 = new StringOrcTest(outputDir, false);
        string1.gen();

        StringOrcTest string2 = new StringOrcTest(outputDir, true);
        string2.gen();
    }

    public OrcTest(Path outputDir, Path path, String schema, CompressionKind compression) throws IOException {
        this.path = new Path(outputDir, path);
        this.schema = schema;
        this.compression = compression;

        TypeDescription schemaDesc = TypeDescription.fromString(schema);
        OrcFile.WriterOptions options = OrcFile.writerOptions(new Configuration()).setSchema(schemaDesc)
                .compress(compression);

        this.writer = OrcFile.createWriter(this.path, options);
        this.batch = schemaDesc.createRowBatch();
        this.row = batch.size;
    }

    protected abstract void writeData() throws IOException;

    protected final void writeBoolean(int column, Boolean value) {
        LongColumnVector vec = (LongColumnVector) this.batch.cols[column];
        if (value == null) {
            vec.vector[this.row] = 0;
            vec.isNull[this.row] = true;
            vec.noNulls = false;
        } else {
            vec.vector[this.row] = value ? 1 : 0;
        }
    }

    protected final void writeLong(int column, Long value) {
        LongColumnVector vec = (LongColumnVector) this.batch.cols[column];
        if (value == null) {
            vec.vector[this.row] = 0;
            vec.isNull[this.row] = true;
            vec.noNulls = false;
        } else {
            vec.vector[this.row] = value;
        }
    }

    protected final void writeString(int column, String value) {
        BytesColumnVector vec = (BytesColumnVector) this.batch.cols[column];
        if (value == null) {
            vec.setVal(this.row, new byte[] {});
            vec.isNull[this.row] = true;
            vec.noNulls = false;
        } else {
            vec.setVal(this.row, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    protected final void finalizeRow() throws IOException {
        this.batch.size += 1;
        if (this.batch.size == this.batch.getMaxSize()) {
            this.writer.addRowBatch(this.batch);
            this.batch.reset();
        }
        this.row = this.batch.size;
    }

    public final void gen() throws IOException {
        this.writeData();
        this.close();
    }

    public final void close() throws IOException {
        if (this.batch.size != 0) {
            this.writer.addRowBatch(this.batch);
        }

        this.writer.close();
    }

    static final class SimpleOrcTest extends OrcTest {
        SimpleOrcTest(Path outputDir, boolean zstd) throws IOException {
            super(outputDir, zstd ? new Path("simple-zstd-01.orc") : new Path("simple-01.orc"), "struct<value:boolean>",
                    zstd ? CompressionKind.ZSTD : CompressionKind.ZLIB);
        }

        protected void writeData() throws IOException {
            this.writeBoolean(0, true);
            this.finalizeRow();
            this.writeBoolean(0, false);
            this.finalizeRow();
            this.writeBoolean(0, false);
            this.finalizeRow();
            this.writeBoolean(0, false);
            this.finalizeRow();
            this.writeBoolean(0, true);
            this.finalizeRow();
            this.writeBoolean(0, null);
            this.finalizeRow();
            this.writeBoolean(0, true);
            this.finalizeRow();
            this.writeBoolean(0, null);
            this.finalizeRow();
            this.writeBoolean(0, null);
            this.finalizeRow();
            this.writeBoolean(0, null);
            this.finalizeRow();
            this.writeBoolean(0, false);
            this.finalizeRow();
            this.writeBoolean(0, null);
            this.finalizeRow();
            this.writeBoolean(0, null);
            this.finalizeRow();
        }
    }

    static final class LongOrcTest extends OrcTest {
        LongOrcTest(Path outputDir, boolean zstd) throws IOException {
            super(outputDir, zstd ? new Path("long-zstd-01.orc") : new Path("long-01.orc"),
                    "struct<value:bigint,is_whatever:boolean>", zstd ? CompressionKind.ZSTD : CompressionKind.ZLIB);
        }

        protected void writeData() throws IOException {
            this.writeLong(0, 263171263L);
            this.writeBoolean(1, false);
            this.finalizeRow();
            this.writeLong(0, 12L);
            this.writeBoolean(1, true);
            this.finalizeRow();
            this.writeLong(0, 12371981672832L);
            this.writeBoolean(1, true);
            this.finalizeRow();
            this.writeLong(0, 3L);
            this.writeBoolean(1, false);
            this.finalizeRow();

            for (long i = 12345; i < 19999; i += 3) {
                this.writeLong(0, i);
                this.writeBoolean(1, i % 2 == 0);
                this.finalizeRow();
            }
            this.writeLong(0, 1237198L);
            this.writeBoolean(1, true);
            this.finalizeRow();
        }
    }

    static final class StringOrcTest extends OrcTest {
        StringOrcTest(Path outputDir, boolean zstd) throws IOException {
            super(outputDir, zstd ? new Path("string-zstd-01.orc") : new Path("string-01.orc"),
                    "struct<id:bigint,name:string,os_whatever:boolean>",
                    zstd ? CompressionKind.ZSTD : CompressionKind.ZLIB);
        }

        protected void writeData() throws IOException {
            this.writeLong(0, 1L);
            this.writeString(1, "foo");
            this.writeBoolean(2, false);
            this.finalizeRow();

            this.writeLong(0, 2L);
            this.writeString(1, "bar");
            this.writeBoolean(2, true);
            this.finalizeRow();

            this.writeLong(0, 3L);
            this.writeString(1, "foo");
            this.writeBoolean(2, true);
            this.finalizeRow();
        }
    }
}
