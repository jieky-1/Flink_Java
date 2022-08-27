import java.util.ArrayList;

public class MyTest {
    public static void main(String[] args){
        // 没有使用匿名内部类
        ArrayList<String> strings1 = new ArrayList<String>();
        // 使用匿名内部类，没有重写父类任何方法
        ArrayList<String> strings2 = new ArrayList<String>() {};
    }
}
