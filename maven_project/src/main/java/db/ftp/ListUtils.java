package db.ftp;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chongwang11
 * @date 2023-05-16 17:41
 * @description
 */
public class ListUtils {
    public static <T> List<List<T>> partition(final List<T> list, final int size) {
        if (list == null) {
            throw new NullPointerException("List must not be null");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Size must be greater than 0");
        }
        return new Partition<>(list, size);
    }

    private static class Partition<T> extends AbstractList<List<T>> {
        private final List<T> list;
        private final int size;

        private Partition(final List<T> list, final int size) {
            this.list = list;
            this.size = size;
        }

        @Override
        public List<T> get(final int index) {
            final int listSize = size();
            if (index < 0) {
                throw new IndexOutOfBoundsException("Index " + index + " must not be negative");
            }
            if (index >= listSize) {
                throw new IndexOutOfBoundsException("Index " + index + " must be less than size " +
                        listSize);
            }
            final int start = index * size;
            final int end = Math.min(start + size, list.size());
            return list.subList(start, end);
        }

        @Override
        public int size() {
            return (int) Math.ceil((double) list.size() / (double) size);
        }

        @Override
        public boolean isEmpty() {
            return list.isEmpty();
        }
    }

    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i <= 10; i++) {
            list.add(i + "");
        }
        //将list集合按照2000条数据分割为一个单独的List集合
        List<List<String>> partition = ListUtils.partition(list, 3);
        for (List<String> strings : partition) {
            System.out.println(String.join(",", strings));
        }
        System.out.println(partition.size());
    }
}