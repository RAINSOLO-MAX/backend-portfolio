package dev.rainsolo.minirpc.loadbalance;

public final class LoadBalances {

    private LoadBalances() {
    }

    public static LoadBalance of(String strategy) {
        if (strategy == null) {
            return new RoundRobinLoadBalance();
        }
        return switch (strategy.toLowerCase()) {
            case "random" -> new RandomLoadBalance();
            case "consistenthash", "consistent_hash" -> new ConsistentHashLoadBalance();
            default -> new RoundRobinLoadBalance();
        };
    }
}
