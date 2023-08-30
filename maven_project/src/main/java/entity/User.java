package entity;

import lombok.Data;

import java.util.Date;

/**
 * @author chongwang11
 * @date 2022 12 07 15 53
 * @description
 */
@Data
public class User {
    Integer id;
    String name;
    Integer age;
    Date birthday;
    String city;

    public User(Integer id, String name, Integer age, Date birthday, String city) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.birthday = birthday;
        this.city = city;
    }

    public User(String name) {
        this.name = name;
    }
}
