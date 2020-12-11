package com.company;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.RecursiveAction;

public class Main {
    public static void main(String[] args) {
        List<CSVRecord> list;
        long start, finish;
        long libSortTime = 0;
        long mergeSortTime = 0;
        int trials = 300;
        String filename = "acs2017_county_data.csv";

        for(int min = 1; min <= 2048; min *= 2) {
            for (int i = 0; i < trials; i++) {
                list = readRecords(filename);
                start = System.nanoTime();
                list.sort(new CompareRatings());
                //PrintRecords.p(list);
                finish = System.nanoTime();
                libSortTime += finish - start;

                list = readRecords(filename);
                MergeSortParallel msp = new MergeSortParallel(list, min);
                start = System.nanoTime();
                msp.sort();
                //PrintRecords.p(list);
                finish = System.nanoTime();
                mergeSortTime += finish - start;
            }
            libSortTime = libSortTime / trials;
            mergeSortTime = mergeSortTime / trials;
            System.out.println("average over " + trials + " trials for min = " + min);
            System.out.println("Collections.sort() time:\t" + libSortTime + " ns");
            System.out.println("Parallel merge sort time:\t" + mergeSortTime + " ns");
            System.out.println();
        }
    }
    private static List<CSVRecord> readRecords(String filename) {
        Reader in = null;
        List<CSVRecord> list = null;
        try {
            in = new FileReader(filename);
            CSVParser parser = new CSVParser( in, CSVFormat.DEFAULT );
            list = parser.getRecords();
        } catch (IOException e) {
            e.printStackTrace();
        }
        list.remove(0);
        return list;
    }
}

class CompareRatings implements Comparator<CSVRecord> {
    public int compare(CSVRecord r1, CSVRecord r2) {
        return compareStatic(r1, r2);
    }

    public static int compareStatic(CSVRecord r1, CSVRecord r2) {
        //return Double.compare(Double.parseDouble(r1.get(0)), Double.parseDouble(r2.get(0)));
        return Integer.parseInt(r1.get(3)) - Integer.parseInt(r2.get(3));
    }
}

class MergeSortParallel extends RecursiveAction {

    //minimum number of elements to be split into two threads, otherwise sorted sequentially
    private int min;

    private List<CSVRecord> list;
    private List<CSVRecord> left;
    private List<CSVRecord> right;


    public MergeSortParallel(List<CSVRecord> list, int min) {
        this.list = list;
        this.min = min;
    }

    public void sort() {
        compute();
    }

    protected void compute() {

        if (list.size() <= min) {
            list.sort(new CompareRatings());
        } else {
            int middle = list.size() / 2;
            left = new ArrayList<CSVRecord>(list.subList(0, middle));
            right = new ArrayList<CSVRecord>(list.subList(middle, list.size()));
            invokeAll(
                    new MergeSortParallel(left, min),
                    new MergeSortParallel(right, min)
            );
            merge();
        }
    }

    private void printAll() {
        //for debugging
        System.out.println("list");
        PrintRecords.p(list);
        System.out.println("left");
        PrintRecords.p(left);
        System.out.println("right");
        PrintRecords.p(right);
        System.out.println();
    }
    private void merge() {
        int i = 0; //index in main list
        int l = 0; //index in left list
        int r = 0; //index in right list
        while(l < left.size() && r < right.size()) {
            if (CompareRatings.compareStatic(left.get(l), right.get(r)) <= 0) {
                list.set(i, left.get(l));
                l++;
            } else {
                list.set(i, right.get(r));
                r++;
            }
            i++;
        }

        while(l < left.size()) {
            list.set(i, left.get(l));
            l++;
            i++;
        }

        while(r < right.size()) {
            list.set(i, right.get(r));
            r++;
            i++;
        }
    }
}

class PrintRecords {
    public static void p(List<CSVRecord> list) {
        for(CSVRecord rr : list) {
            System.out.println(rr.get(2) + "   \t" + rr.get(3));
        }
    }
}

/*
class MergeSortParallelInPlace extends RecursiveAction {

//minimum number of elements to be split into two threads, otherwise sorted sequentially
private static final int MIN = 64;

private List<CSVRecord> list;
private int low;
private int high;
private int middle;

public MergeSortParallel(List<CSVRecord> list, int low, int high) {
this.list = list;
this.low = low;
this.high = high;
}

public MergeSortParallel(List<CSVRecord> list) {
this(list, 0, list.size() - 1);
}

public void sort() {
compute();
}

protected void compute() {
if (low < high) {
int size = high - low;
if (size <= 1) {
return;
} else {
middle = low + size / 2;
invokeAll(
new MergeSortParallel(list, low, middle),
new MergeSortParallel(list, middle + 1, high)
);
merge();
}
}
}

private void merge() {
List<CSVRecord> left = list.subList(low, middle);
List<CSVRecord> right = list.subList(middle + 1, high);

int i;
for(i = low; left.size() > 0 && right.size() > 0; i++) {
if(CompareRatings.compareStatic(left.get(0), right.get(0)) <= 0) {
list.set(i, left.remove(0));
} else {
list.set(i, right.remove(0));
}
}

for(; left.size() > 0; i++) {
list.set(i, left.remove(0));
}

for(; right.size() > 0; i++) {
list.set(i, right.remove(0));
}


}
}
*/

