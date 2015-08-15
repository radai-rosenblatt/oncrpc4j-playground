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
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.GrizzlyFuture;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
public class LeakTest {
    public static final int N_THREADS = 10;
    private static volatile boolean die = false;
    private static volatile Throwable causeOfDeath = null;

    private InetAddress localhostAddress;
    private MetricRegistry metrics = new MetricRegistry();
    private Meter requests = metrics.meter("requests");
    private Meter bindFailures = metrics.meter("bindFailures");
    private Counter opens = metrics.counter("opens");
    private Counter closes = metrics.counter("closes");
    private ConsoleReporter reporter;

    @Before
    public void setup() throws Throwable {
        localhostAddress = InetAddress.getByName("127.0.0.1");
        try (OncRpcClient rpcClient = new OncRpcClient(localhostAddress, IpProtocolType.TCP, 111)) {
            XdrTransport transport = rpcClient.connect();
            GenericPortmapClient portmapClient = new GenericPortmapClient(transport);
            String uaddr = InetSocketAddresses.uaddrOf("127.0.0.1", 666);
            portmapClient.setPort(666, 666, "tcp", uaddr, "bob");
        } catch (Throwable t) {
            Throwable cause = getRootCause(t);
            if (cause instanceof ConnectException) {
                Assume.assumeNoException("rpcbind should be running", cause);
            }
            throw t;
        }
        reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.SECONDS);
    }

    @After
    public void teardown() throws Exception {
        if (reporter != null) {
            reporter.stop();
        }
    }

    @Test
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
            Future<Void> future = executor.submit(new GrizzlyConnectTask(inetSocketAddress, requests, bindFailures, opens, closes));
            futures.add(future);
        }
        for (Future<Void> future : futures) {
            future.get(); //block
        }
        throw causeOfDeath;
    }

    private static class GrizzlyConnectTask implements Callable<Void> {
        private final InetSocketAddress address;
        private final Meter requests;
        private final Meter bindFailures;
        private final Counter opens;
        private final Counter closes;

        public GrizzlyConnectTask(InetSocketAddress address, Meter requests, Meter bindFailures, Counter opens, Counter closes) {
            this.address = address;
            this.requests = requests;
            this.bindFailures = bindFailures;
            this.opens = opens;
            this.closes = closes;
        }

        @Override
        public Void call() throws Exception {
            while (!die) {
                TCPNIOTransport transport = null;
                try {
                    transport = TCPNIOTransportBuilder.newInstance().build();
                    transport.start();
                    GrizzlyFuture<Connection> connectFuture = transport.connect(address);
                    connectFuture.get(); //block
                    opens.inc(); //successful open
                    requests.mark();
                } catch (Throwable t) {
                    //noinspection ThrowableResultOfMethodCallIgnored
                    Throwable root = getRootCause(t);
                    if (root instanceof BindException) {
                        bindFailures.mark(); //ephemeral port exhaustion.
                        continue;
                    }
                    causeOfDeath = t;
                    die = true;
                } finally {
                    if (transport != null) {
                        transport.shutdown().get(); //block
                        closes.inc(); //successful close
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
                    Throwable root = getRootCause(t);
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

    private static Throwable getRootCause(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
