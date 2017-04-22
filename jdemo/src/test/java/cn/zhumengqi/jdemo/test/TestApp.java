package cn.zhumengqi.jdemo.test;
import cn.zhumengqi.jdemo.TestRunnable;

/**
 * Created by zhumengqi on 17-4-12.
 */
public class TestApp {

    public static void main(String[] args){
//      创建一个类对象
        Runnable oneRunnable = new TestRunnable();

//      由Runnable创建Thread对象
        Thread oneThread = new Thread(oneRunnable);
        oneThread.start();
    }
}
