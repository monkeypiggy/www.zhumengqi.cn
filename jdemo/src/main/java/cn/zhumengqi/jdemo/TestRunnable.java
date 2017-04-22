package cn.zhumengqi.jdemo;

/**
 * Created by mike on 17-4-12.
 */
public class TestRunnable implements Runnable {
    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    public void run() {
        System.out.println("多线程进入睡眠...");
        try {
            printContent("Hello world!");
            Thread.sleep(30000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("多线程执行完毕...");
    }
    
    private void printContent(String content){
        System.out.println(content);
    }
}
