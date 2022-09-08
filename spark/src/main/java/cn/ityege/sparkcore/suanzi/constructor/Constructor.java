package cn.ityege.sparkcore.suanzi.constructor;

import com.google.gson.Gson;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class Constructor {
    public static void main(String[] args) throws Exception {
        List<Person> list = new ArrayList<>();
        list.add(new Person("张三", 18));
        list.add(new Person("李四", 28));
        list.add(new Person("王五", 89));
        list.add(new Person("赵六", 118));
        list.add(new Person("田七", 189));
        Gson gson = new Gson();
        String s = gson.toJson(list);
        System.out.println(s);
        File file = new File("person.json");
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        fileOutputStream.write(s.getBytes(StandardCharsets.UTF_8));
    }
}

class Person {
    private String name;
    private Integer age;

    public Person() {
    }

    public Person(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
