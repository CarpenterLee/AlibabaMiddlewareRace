package gzx.cache.file;

/**
 * Created by gzx on 16-7-13.
 */
public class Block {
    private String fileName;
    private long blkNum;

    public Block(String fileName, long blkNum) {
        this.fileName = fileName;
        this.blkNum = blkNum;
    }

    public String fileName() {
        return fileName;
    }

    public long number() {
        return blkNum;
    }
    @Override
    public boolean equals(Object obj) {
        Block blk = (Block) obj;
        return blkNum == blk.blkNum && fileName.equals(blk.fileName);
    }
    @Override
    public String toString() {
        return "[file " + fileName + ", block " + blkNum + "]";
    }
    @Override
    public int hashCode() {
        int res= fileName.hashCode();
        res += 31 * blkNum;
        return res;
//        return toString().hashCode();
    }
}