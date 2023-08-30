package study.collection;

import entity.User;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author chongwang11
 * @date 2022 12 07 14 52
 * @description
 */
public class StreamLearning {

    public static void main(String[] args) {
        List<User> arrylist = Arrays.asList(
                new User("a,c"),
                new User("b"),
                new User("a,c"),
                new User("a,b"));
        Map<String, List<User>> groupConfig = arrylist
                .stream()
                .collect(Collectors.groupingBy(x -> Arrays.stream(x.getName().split(",")).sorted().collect(Collectors.joining(","))));
        System.out.println(groupConfig);
    }
}
