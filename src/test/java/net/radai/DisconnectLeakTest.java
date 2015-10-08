package net.radai;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.dcache.utils.net.InetSocketAddresses;
import org.dcache.xdr.IpProtocolType;
import org.dcache.xdr.OncRpcClient;
import org.dcache.xdr.XdrTransport;
import org.dcache.xdr.portmap.GenericPortmapClient;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.strategies.SameThreadIOStrategy;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Radai Rosenblatt
 * @version Aug 15, 2015
 * @since Phase1
 */
public class DisconnectLeakTest {
    public static final int N_THREADS = 10;
    private static volatile boolean die = false;
    private static volatile Throwable causeOfDeath = null;

    private InetAddress localhostAddress;
    private MetricRegistry metrics = new MetricRegistry();
    private Meter requests = metrics.meter("requests");
    private Meter bindFailures = metrics.meter("bindFailures");
    private Counter successfulOpens = metrics.counter("successfulOpens");
    private Counter failedOpens = metrics.counter("failedOpens");
    private Counter successfulCloses = metrics.counter("successfulCloses");
    private Counter failedCloses = metrics.counter("failedCloses");
    private ConsoleReporter reporter;

    @Before
    public void setup() throws Throwable {
        localhostAddress = InetAddress.getByName("127.0.0.1");
        try (OncRpcClient rpcClient = new OncRpcClient(localhostAddress, IpProtocolType.TCP, 111)) {
            XdrTransport transport = rpcClient.connect();
            GenericPortmapClient portmapClient = new GenericPortmapClient(transport);
            String uaddr = InetSocketAddresses.uaddrOf("127.0.0.1", 666);
            portmapClient.setPort(666, 666, "tcp", uaddr, "bob"); //register a bogus "bob" application on port 666
        } catch (Throwable t) {
            Throwable cause = Util.getRootCause(t);
            if (cause instanceof ConnectException) {
                Assume.assumeNoException("rpcbind should be running", cause);
            }
            throw t;
        }
        reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(10, TimeUnit.SECONDS);
    }

    @After
    public void teardown() throws Exception {
        if (reporter != null) {
            reporter.stop();
        }
    }

//    @Test
    public void testLeakWithPortmapClient() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);
        Set<Future<Void>> futures = new HashSet<>();
        for (int i = 0; i < N_THREADS; i++) {
            Future<Void> future = executor.submit(new PortmapQueryTask(localhostAddress, requests, bindFailures));
            futures.add(future);
        }
        for (Future<Void> future : futures) {
            future.get(); //block
        }
        throw causeOfDeath;
    }

    @Test
    public void testLeakWithGrizzly() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);
        Set<Future<Void>> futures = new HashSet<>();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(localhostAddress, 111);
        for (int i = 0; i < N_THREADS; i++) {
            Future<Void> future = executor.submit(new GrizzlyConnectTask(inetSocketAddress, requests, bindFailures, successfulOpens, failedOpens, successfulCloses, failedCloses, 0));
            futures.add(future);
        }
        for (Future<Void> future : futures) {
            future.get(); //block
        }
        Thread.sleep(1000); //let everything calm down
        reporter.report();
        throw causeOfDeath;
    }

//    @Test
    public void testSlowLeakWithGrizzly() throws Throwable {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Set<Future<Void>> futures = new HashSet<>();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(localhostAddress, 111);
        //on my machine 1 thread wit a 2 milli sleep interval results in no bind exceptions
        executor.submit(new GrizzlyConnectTask(inetSocketAddress, requests, bindFailures, successfulOpens, failedOpens, successfulCloses, failedCloses, 2)).get();
        Thread.sleep(1000); //let everything calm down
        reporter.report();
        throw causeOfDeath;
    }

    @Test
    public void testLeakWithNio() throws Throwable {
//        SelectorProvider provider = SelectorProvider.provider();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(localhostAddress, 111);
        while (true) {
            Selector selector = Selector.open();
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(false);
            SelectionKey key = socketChannel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            try {
                socketChannel.connect(inetSocketAddress);
                int numReadyChannels = selector.select();
                Set<SelectionKey> readyKeys = selector.selectedKeys();
                for (SelectionKey k : readyKeys) {
                    if (k.isConnectable()) {
                        successfulOpens.inc();
                        requests.mark();
                        //connection established
                    }
                }
//            AbstractSelector selector = provider.openSelector();
                selector.close();
                socketChannel.close();
                successfulCloses.inc();
            } catch (BindException e) {
                failedOpens.inc();
                bindFailures.mark();
                selector.close();
                socketChannel.close();
                successfulCloses.inc();
            }
        }
    }

    private static class GrizzlyConnectTask implements Callable<Void> {
        private final InetSocketAddress address;
        private final Meter requests;
        private final Meter bindFailures;
        private final Counter successfulOpens;
        private final Counter failedOpens;
        private final Counter successfulCloses;
        private final Counter failedCloses;
        private final long sleepMillis;

        public GrizzlyConnectTask(InetSocketAddress address, Meter requests, Meter bindFailures, Counter successfulOpens, Counter failedOpens, Counter successfulCloses, Counter failedCloses, long sleepMillis) {
            this.address = address;
            this.requests = requests;
            this.bindFailures = bindFailures;
            this.successfulOpens = successfulOpens;
            this.failedOpens = failedOpens;
            this.successfulCloses = successfulCloses;
            this.failedCloses = failedCloses;
            this.sleepMillis = sleepMillis;
        }

        @Override
        public Void call() throws Exception {
            while (!die) {
                if (sleepMillis > 0) {
                    Thread.sleep(sleepMillis);
                }
                TCPNIOTransport transport = null;
                boolean opened = false;
                try {
                    transport = TCPNIOTransportBuilder.newInstance().setIOStrategy(SameThreadIOStrategy.getInstance()).build();
                    transport.start();
                    transport.connect(address).get(); //block
                    opened = true;
                    successfulOpens.inc(); //successful open
                    requests.mark();
                } catch (Throwable t) {
                    //noinspection ThrowableResultOfMethodCallIgnored
                    Throwable root = Util.getRootCause(t);
                    if (root instanceof BindException) {
                        bindFailures.mark(); //ephemeral port exhaustion.
                        continue;
                    }
                    causeOfDeath = t;
                    die = true;
                } finally {
                    if (!opened) {
                        failedOpens.inc();
                    }
                    if (transport != null) {
                        try {
                            transport.shutdown().get(); //block
                            successfulCloses.inc(); //successful close
                        } catch (Throwable t) {
                            failedCloses.inc();
                            System.err.println("while trying to close transport");
                            t.printStackTrace();
                        }
                    } else {
                        //no transport == successful close
                        successfulCloses.inc();
                    }
                }
            }
            return null;
        }
    }

    private static class PortmapQueryTask implements Callable<Void> {
        private final InetAddress address;
        private final Meter requests;
        private final Meter bindFailures;

        public PortmapQueryTask(InetAddress address, Meter requests, Meter bindFailures) {
            this.address = address;
            this.requests = requests;
            this.bindFailures = bindFailures;
        }

        @Override
        public Void call() throws Exception {
            while (!die) {
                try (OncRpcClient rpcClient = new OncRpcClient(address, IpProtocolType.TCP, 111)) {
                    XdrTransport transport = rpcClient.connect();
                    GenericPortmapClient portmapClient = new GenericPortmapClient(transport);
                    String port = portmapClient.getPort(666, 666, "tcp");
//                System.err.println("got " + port);
                    requests.mark();
                } catch (Throwable t) {
                    //noinspection ThrowableResultOfMethodCallIgnored
                    Throwable root = Util.getRootCause(t);
                    if (root instanceof BindException) {
                        bindFailures.mark(); //ephemeral port exhaustion.
                        continue;
                    }
                    causeOfDeath = t;
                    die = true;
                }
            }
            return null;
        }
    }
}
