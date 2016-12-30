package com.mtunique.testmem;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MathUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by taomeng on 2016/12/29.
 */
public class WriteView extends AbstractPagedOutputView {

    private final List<MemorySegment> pages;

    private final MemorySegmentSource memSource;

    private final int sizeBits;

    private final int sizeMask;

    private int currentPageNumber;

    private int segmentNumberOffset;

    public WriteView(List<MemorySegment> pages, MemorySegmentSource memSource, int pageSize)
    {
        super(pages.get(0), pageSize, 0);

        this.pages = pages;
        this.memSource = memSource;
        this.sizeBits = MathUtils.log2strict(pages.get(0).size());
        this.sizeMask = pageSize - 1;
        this.segmentNumberOffset = 0;
    }


    @Override
    protected MemorySegment nextSegment(MemorySegment current, int bytesUsed) throws IOException {
        MemorySegment next = this.memSource.nextSegment();
        if(next == null) {
            throw new EOFException();
        }
        this.pages.add(next);

        this.currentPageNumber++;
        return next;
    }

    private long getCurrentPointer() {
        return (((long) this.currentPageNumber) << this.sizeBits) + getCurrentPositionInSegment();
    }

    public int resetTo(long pointer) {
        final int pageNum  = (int) (pointer >>> this.sizeBits);
        final int offset = (int) (pointer & this.sizeMask);

        this.currentPageNumber = pageNum;

        int posInArray = pageNum - this.segmentNumberOffset;
        seekOutput(this.pages.get(posInArray), offset);

        return posInArray;
    }

    @SuppressWarnings("unused")
    public void setSegmentNumberOffset(int offset) {
        this.segmentNumberOffset = offset;
    }
}

