package com.datastax.driver.core;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

import static com.datastax.driver.core.Assertions.assertThat;

public class SpeculativeExecutionTest {
    CCMBridge ccm = null;
    Scassandra scassandra = null;
    Cluster cluster = null;
    SortableLoadBalancingPolicy loadBalancingPolicy;
    Session session;
    PrimingClient mockHost1;
    Counter speculativeExecutions;

    @BeforeClass(groups = "short")
    public void setup() {
        // Start a regular CCM cluster and stop host1
        String clusterName = "speculative_execution";
        ccm = CCMBridge.create(clusterName, 3);
        ccm.stop(1);
        ccm.waitForDown(1);

        // Put an SCassandra instance in host1's place
        String address = CCMBridge.ipOfNode(1);
        scassandra = ScassandraFactory.createServer(address, 9042, address, TestUtils.findAvailablePort(9043));
        scassandra.start();
        mockHost1 = PrimingClient.builder()
            .withHost(address).withPort(scassandra.getAdminPort())
            .build();

        // Mock cluster name since it will be checked when the driver tries to connect
        mockHost1.prime(
            PrimingRequest.queryBuilder()
                .withQuery("select cluster_name from system.local")
                .withRows(ImmutableMap.of("cluster_name", clusterName))
                .build()
        );

        // Use a relatively large delay for the scenarios where we want retries before the next speculative execution is triggered
        int speculativeExecutionDelay = 500;

        loadBalancingPolicy = new SortableLoadBalancingPolicy();
        cluster = Cluster.builder()
            .addContactPoint(CCMBridge.ipOfNode(2))
            .withLoadBalancingPolicy(loadBalancingPolicy)
            .withSpeculativeExecutionPolicy(new ConstantSpeculativeExecutionPolicy(1, speculativeExecutionDelay))
            .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
            .build();

        speculativeExecutions = cluster.getMetrics().getErrorMetrics().getSpeculativeExecutions();

        session = cluster.connect();
        loadBalancingPolicy.order = ImmutableList.of(2, 3, 1);

        session.execute("create keyspace test with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("use test");
        session.execute("create table foo(k int primary key, v text)");
        session.execute("insert into foo (k, v) values (1, 'value from real host')");
    }

    @Test(groups = "short")
    public void should_not_start_speculative_execution_if_first_execution_completes_successfully() {
        loadBalancingPolicy.order = ImmutableList.of(2, 3, 1);
        long startCount = speculativeExecutions.getCount();

        ResultSet rs = session.execute("select v from foo where k = 1");
        Row row = rs.one();

        assertThat(row.getString("v")).isEqualTo("value from real host");
        assertThat(speculativeExecutions.getCount()).isEqualTo(startCount);
        assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(TestUtils.findHost(cluster, 2));
    }

    @Test(groups = "short")
    public void should_not_start_speculative_execution_if_first_execution_completes_with_error() {
        loadBalancingPolicy.order = ImmutableList.of(2, 3, 1);
        long startCount = speculativeExecutions.getCount();

        try {
            session.execute("select v from foo where k"); // syntax error
        } catch (SyntaxError error) { /* expected */ }

        assertThat(speculativeExecutions.getCount()).isEqualTo(startCount);
    }

    @Test(groups = "short")
    public void should_not_start_speculative_execution_if_first_execution_retries_but_is_still_fast_enough() {
        loadBalancingPolicy.order = ImmutableList.of(2, 3, 1);
        Counter retriesOnUnavailable = cluster.getMetrics().getErrorMetrics().getRetriesOnUnavailable();
        long startCount = speculativeExecutions.getCount();
        long retriesStartCount = retriesOnUnavailable.getCount();

        SimpleStatement statement = new SimpleStatement("select v from foo where k = 1");
        // This will fail because our keyspace has RF = 1, but we use the downgrading retry policy so there should be one retry.
        statement.setConsistencyLevel(ConsistencyLevel.TWO);
        ResultSet rs = session.execute(statement);
        Row row = rs.one();

        assertThat(row.getString("v")).isEqualTo("value from real host");
        assertThat(speculativeExecutions.getCount()).isEqualTo(startCount);
        assertThat(retriesOnUnavailable.getCount()).isEqualTo(retriesStartCount + 1);
        assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(TestUtils.findHost(cluster, 2));
    }

    @Test(groups = "short")
    public void should_start_speculative_execution_if_first_execution_takes_too_long() {
        loadBalancingPolicy.order = ImmutableList.of(1, 2, 3);
        mockHost1.prime(
            PrimingRequest.queryBuilder()
                .withQuery("select v from foo where k = 1")
                .withFixedDelay(2000)
                .withRows(ImmutableMap.of("v", "mock value from host1"))
                .build()
        );
        long startCount = speculativeExecutions.getCount();

        ResultSet rs = session.execute("select v from foo where k = 1");
        Row row = rs.one();

        assertThat(row.getString("v")).isEqualTo("value from real host");
        assertThat(speculativeExecutions.getCount()).isEqualTo(startCount + 1);
        assertThat(rs.getExecutionInfo().getQueriedHost()).isEqualTo(TestUtils.findHost(cluster, 2));
    }

    @AfterClass(groups = "short")
    public void teardown() {
        if (cluster != null)
            cluster.close();
        if (scassandra != null)
            scassandra.stop();
        if (ccm != null)
            ccm.remove();
    }

    /**
     * A load balancing policy where the order of the query plan can be customized based
     * on the last byte of the address.
     */
    static class SortableLoadBalancingPolicy implements LoadBalancingPolicy {

        volatile List<Integer> order = ImmutableList.of(1, 2, 3);

        private final Set<Host> hosts = new CopyOnWriteArraySet<Host>();

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            this.hosts.addAll(hosts);
        }

        @Override
        public HostDistance distance(Host host) {
            return HostDistance.LOCAL;
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            Host[] queryPlan = new Host[order.size()];
            int i = 0;
            for (Integer lastByte : order) {
                for (Host host : hosts) {
                    byte[] address = host.getAddress().getAddress();
                    if (address[address.length - 1] == lastByte) {
                        queryPlan[i] = host;
                        break;
                    }
                }
                i += 1;
            }
            return Lists.newArrayList(queryPlan).iterator();
        }

        @Override
        public void onAdd(Host host) {
            onUp(host);
        }

        @Override
        public void onUp(Host host) {
            hosts.add(host);
        }

        @Override
        public void onDown(Host host) {
            hosts.remove(host);
        }

        @Override
        public void onRemove(Host host) {
            onDown(host);
        }

        @Override
        public void onSuspected(Host host) {
        }
    }
}
