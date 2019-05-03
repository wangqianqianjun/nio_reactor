import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author wangqianjun
 *         Created by dell-pc on 2019/5/2.
 */
public class NIOReactorServer {

    /**
     * 处理业务操作的线程
     */
    private static ExecutorService workPool = Executors.newCachedThreadPool();
    /**
     * 主reactor负责监听连接，accept
     */
    private ReactorThread[] mainReactor=new ReactorThread[1];
    /**
     * 子reactor负责处理IO读写发送
     */
    private ReactorThread[] subReactor;
    /**
     * 只有一个serverSocketChannel来监听断开
     */
    private ServerSocketChannel serverSocketChannel;
    private int subNext = 0;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        NIOReactorServer nioReactorServer=new NIOReactorServer();
        nioReactorServer.eventLoopGroup(8);
        nioReactorServer.initAndRegister(8080);

    }

    public void eventLoopGroup(int subThreads) throws IOException {
        subReactor = new ReactorThread[subThreads];
        /**
         * 初始化构造主reactor
         */
        initMainReactor();
        /**
         * 初始化构造子reactor
         */
        initSubReactor();
    }


    private void initMainReactor() throws IOException {
        for (int i = 0; i < mainReactor.length; i++) {
            String threadName = "mainReactor :" + i;
            mainReactor[i] = new ReactorThread(threadName) {
                @Override
                void handler(SelectableChannel channel) throws IOException, ExecutionException, InterruptedException {
                    SocketChannel clientChannel = ((ServerSocketChannel) channel).accept();
                    ReactorThread sub = subReactor[subNext++ % subReactor.length];
                    System.out.println(" 主线程 ：" + getName() + " 收到一个连接事件,交给子线程处理，子线程：" + sub.getName());
                    clientChannel.configureBlocking(false);
                    sub.doStart();
                    sub.register(clientChannel, SelectionKey.OP_READ);
                }
            };
        }
    }

    private void initSubReactor() throws IOException {
        for (int i = 0; i < subReactor.length; i++) {
            String threadName = "subReactor:" + i;
            subReactor[i] = new ReactorThread(threadName) {
                @Override
                void handler(SelectableChannel channel) throws IOException {
                    SocketChannel clientChannel = (SocketChannel) channel;
                    ByteBuffer byteBuffers = ByteBuffer.allocateDirect(1024);
                    /**
                     * 判断客户端是否断开连接，防止服务端空轮询
                     * 因为客户端断开连接之后，服务端建立的SocketChannel 也是会收到可读事件的。
                     * 但是SocketChannel.read(byteBuffers)值是-1
                     */
                    if(clientChannel.read(byteBuffers) == -1){
                        System.out.println("客户端退出，服务端SocketChannel 即将关闭！");
                        clientChannel.close();
                        return;
                    }

                    while (clientChannel.isOpen() && clientChannel.read(byteBuffers) != -1) {
                        if (byteBuffers.position() > 0) {
                            break;
                        }
                    }

                    /**
                     * 没有数据执行后面逻辑
                     */
                    if (byteBuffers.position() == 0) {
                        return;
                    }

                    byteBuffers.flip();

                    byte[] bytes = new byte[byteBuffers.limit()];

                    byteBuffers.get(bytes);

                    //业务处理
                    workPool.submit(() -> {
                        //TODO 业务处理
                    });


                    System.out.println(new String(bytes));
                    System.out.println("子reactor ：" + getName() + " 处理数据,来自：" + clientChannel.getRemoteAddress());
                    // 响应结果 200
                    String response = "HTTP/1.1 200 OK\r\n" +
                            "Content-Length: 11\r\n\r\n" +
                            "Hello World";
                    ByteBuffer buffer = ByteBuffer.wrap(response.getBytes());
                    while (buffer.hasRemaining()) {
                        clientChannel.write(buffer);
                    }

                }
            };
        }
    }

    public void initAndRegister(int port) throws IOException, ExecutionException, InterruptedException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        //获取一个主线程
        ReactorThread main = mainReactor[0];
        main.doStart();
        //主线程只负责accept事件
        main.register(serverSocketChannel, SelectionKey.OP_ACCEPT);
        serverSocketChannel.socket().bind(new InetSocketAddress(port));
    }


    /**
     * ReactorThread 类
     * 模板方法，具体handler交给具体业务实现
     */
    abstract class ReactorThread extends Thread {
        boolean start = false;
        private Selector selector;
        /**
         * 每个线程只关注自己的注册事件
         */
        private int interestOps;
        /**
         *
         */
        LinkedBlockingQueue<Runnable> taskQueue = new LinkedBlockingQueue<>();


        public ReactorThread(String threadName) throws IOException {
            selector = Selector.open();
            this.setName(threadName);
        }

        abstract void handler(SelectableChannel channel) throws IOException, ExecutionException, InterruptedException;

        @Override
        public void run() {
            while (start) {

                /**
                 * 执行队列中的任务，进行注册
                 */
                Runnable task;
                while ((task = taskQueue.poll()) != null) {
                    task.run();
                }

                try {
                    selector.select(100);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // 获取事件
                Set<SelectionKey> selectionKeys = selector.selectedKeys();

                // 迭代
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    iterator.remove();
                    int readyOps = key.readyOps();
                    //关注注册进来的事件
                    if ((readyOps & interestOps) != 0) {
                        System.out.println("线程："+getName()+"收到一个新的事件连接,事件类型："+readyOps);
                        SelectableChannel channel = (SelectableChannel) key.attachment();
                        //交给handler处理
                        try {
                            handler(channel);
                            if (!channel.isOpen()) {
                                /**
                                 * 如果channel关闭了，取消该key订阅
                                 */
                                key.cancel();
                            }
                        } catch (IOException e) {
                            /**
                             * 如果发生异常，取消该key订阅
                             */
                           key.cancel();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        public SelectionKey register(SelectableChannel channel, int ops) throws ExecutionException, InterruptedException {
            interestOps = ops;
            /**
             * 为什么放到任务里面，然后交给对象本身线程去执行呢
             * 因为register操作在nio源码SelectorImpl里面，会对this.publicKeys进行加锁
             * 而主线程的selector.select()操作，在SelectorImpl里面，会调用lockAndDoSelect这个方法，
             * 这个方法里面也会对this.publicKeys进行加锁。所以会有很大的资源争夺情况发生。
             * 因此交给主线程去处理
             */
            FutureTask<SelectionKey> futureTask = new FutureTask<>(() -> channel.register(selector, ops, channel));
            taskQueue.add(futureTask);
            System.out.println("线程："+getName()+" 绑定了一个通道,事件："+ops);
            return futureTask.get();
        }

        public void doStart() {
            if (!start) {
                System.out.println("开启一个thread :"+getName());
                start();
                start = true;
            }
        }
    }


}
