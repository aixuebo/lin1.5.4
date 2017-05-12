package org.apache.kylin.cube;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * maming自己写的,用于测试segment的范围的demo
 */
public class CubeSegmentCompareTest implements Comparable<CubeSegmentCompareTest>{

    private long dateRangeStart;
    private long dateRangeEnd;

    public CubeSegmentCompareTest(String dateRangeStart,String dateRangeEnd){
    	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    	try {
			this.dateRangeStart = df.parse(dateRangeStart).getTime();
			this.dateRangeEnd = df.parse(dateRangeEnd).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
    }

	@Override
	public int compareTo(CubeSegmentCompareTest other) {
		  long comp = this.getDateRangeStart() - other.getDateRangeStart();
	        if (comp != 0)
	            return comp < 0 ? -1 : 1;

	        comp = this.getDateRangeEnd() - other.getDateRangeEnd();
	        if (comp != 0)
	            return comp < 0 ? -1 : 1;
	        else
	            return 0;
	}

	public long getDateRangeStart() {
		return dateRangeStart;
	}

	public void setDateRangeStart(long dateRangeStart) {
		this.dateRangeStart = dateRangeStart;
	}

	public long getDateRangeEnd() {
		return dateRangeEnd;
	}

	public void setDateRangeEnd(long dateRangeEnd) {
		this.dateRangeEnd = dateRangeEnd;
	}

	public static void checkfitInSegments(List<CubeSegmentCompareTest> segments, CubeSegmentCompareTest newOne){
        //第一个和最后一个CubeSegment
		CubeSegmentCompareTest first = segments.get(0);
		CubeSegmentCompareTest last = segments.get(segments.size() - 1);
     
        //新的segment
        long start = newOne.getDateRangeStart();
        long end = newOne.getDateRangeEnd();
        
        
        boolean startFit = false;
        boolean endFit = false;
        for (CubeSegmentCompareTest sss : segments) {//循环每一个segment
            if (sss == newOne){
            	System.out.println("aaa");
            	continue;
            }
            startFit = startFit || (start == sss.getDateRangeStart() || start == sss.getDateRangeEnd());
            endFit = endFit || (end == sss.getDateRangeStart() || end == sss.getDateRangeEnd());
        }
        
        System.out.println(startFit+"===="+endFit);
        if (!startFit && endFit && newOne == first)
            startFit = true;
        if (!endFit && startFit && newOne == last)
            endFit = true;
        
        System.out.println(startFit+"===="+endFit);
	}

    public boolean dateRangeOverlaps(CubeSegmentCompareTest seg) {
        return dateRangeStart < seg.dateRangeEnd && seg.dateRangeStart < dateRangeEnd;
    }
    
	public static void main(String[] args) {

		List<CubeSegmentCompareTest> list = new ArrayList<CubeSegmentCompareTest>();
		list.add(new CubeSegmentCompareTest("2017-01-08","2017-01-13"));
		list.add(new CubeSegmentCompareTest("2017-01-05","2017-01-08"));
		list.add(new CubeSegmentCompareTest("2017-01-15","2017-01-20"));

		Collections.sort(list);
		for(CubeSegmentCompareTest a :list){
			System.out.println(a.getDateRangeStart()+"=="+a.getDateRangeEnd());
		}
		
		System.out.println("------");
		
		checkfitInSegments(list,new CubeSegmentCompareTest("2017-01-05","2017-01-21"));
		System.out.println("------");
		
		System.out.println(new CubeSegmentCompareTest("2017-01-05","2017-01-21").dateRangeOverlaps(new CubeSegmentCompareTest("2017-01-05","2017-01-21")));
		System.out.println(new CubeSegmentCompareTest("2017-01-05","2017-01-21").dateRangeOverlaps(new CubeSegmentCompareTest("2017-01-08","2017-01-19")));
		System.out.println(new CubeSegmentCompareTest("2017-01-05","2017-01-21").dateRangeOverlaps(new CubeSegmentCompareTest("2017-01-05","2017-01-22")));
		System.out.println(new CubeSegmentCompareTest("2017-01-05","2017-01-21").dateRangeOverlaps(new CubeSegmentCompareTest("2017-01-04","2017-01-21")));
		
		System.out.println(new CubeSegmentCompareTest("2017-01-05","2017-01-21").dateRangeOverlaps(new CubeSegmentCompareTest("2017-01-19","2017-01-25")));
		System.out.println(new CubeSegmentCompareTest("2017-01-05","2017-01-21").dateRangeOverlaps(new CubeSegmentCompareTest("2017-01-22","2017-01-25")));
	}

}
