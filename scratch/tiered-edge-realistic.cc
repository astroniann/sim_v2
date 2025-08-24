// scratch/tiered-edge-realistic.cc
// Comprehensive Realistic Tiered Edge Caching with LRU, Computational Tasks, and Network Metrics

#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/applications-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/random-variable-stream.h"
#include "ns3/flow-monitor-module.h"
#include "ns3/traffic-control-module.h"
#include "ns3/assert.h"

#include <fstream>
#include <filesystem>
#include <random>
#include <unordered_map>
#include <list>
#include <queue>
#include <memory>
#include <algorithm>
#include <functional>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE ("TieredEdgeRealistic");

// ============================================================================
// LRU Cache Implementation
// ============================================================================

class LRUCache {
private:
    struct CacheEntry {
        uint32_t contentId;
        uint64_t size;
        double popularity;
        Time lastAccess;
        uint32_t accessCount;
        
        CacheEntry(uint32_t id, uint64_t sz) : contentId(id), size(sz), 
                                              popularity(0.0), accessCount(0) {}
    };
    
    std::unordered_map<uint32_t, std::list<CacheEntry>::iterator> cacheMap;
    std::list<CacheEntry> cacheList;
    uint64_t maxSize;
    uint64_t currentSize;
    double popularityDecay;
    
public:
    LRUCache(uint64_t size, double decay = 0.95) : maxSize(size), currentSize(0), popularityDecay(decay) {}
    
    bool get(uint32_t contentId) {
        auto it = cacheMap.find(contentId);
        if (it != cacheMap.end()) {
            // Cache hit - move to front and update popularity
            auto& entry = *(it->second);
            entry.lastAccess = Simulator::Now();
            entry.accessCount++;
            entry.popularity = entry.popularity * popularityDecay + (1.0 - popularityDecay);
            
            cacheList.splice(cacheList.begin(), cacheList, it->second);
            return true;
        }
        return false;
    }
    
    void put(uint32_t contentId, uint64_t size) {
        // Check if content already exists
        auto it = cacheMap.find(contentId);
        if (it != cacheMap.end()) {
            // Update existing entry
            auto& entry = *(it->second);
            entry.lastAccess = Simulator::Now();
            entry.accessCount++;
            entry.popularity = entry.popularity * popularityDecay + (1.0 - popularityDecay);
            cacheList.splice(cacheList.begin(), cacheList, it->second);
            return;
        }
        
        // Evict if necessary
        while (currentSize + size > maxSize && !cacheList.empty()) {
            auto& lastEntry = cacheList.back();
            currentSize -= lastEntry.size;
            cacheMap.erase(lastEntry.contentId);
            cacheList.pop_back();
        }
        
        // Add new entry
        if (size <= maxSize) {
            cacheList.emplace_front(contentId, size);
            cacheMap[contentId] = cacheList.begin();
            currentSize += size;
        }
    }
    
    uint64_t getSize() const { return currentSize; }
    uint64_t getMaxSize() const { return maxSize; }
    size_t getEntryCount() const { return cacheMap.size(); }
    
    std::vector<uint32_t> getPopularContents(uint32_t limit = 10) {
        std::vector<std::pair<uint32_t, double>> contents;
        for (const auto& entry : cacheList) {
            contents.emplace_back(entry.contentId, entry.popularity);
        }
        
        std::sort(contents.begin(), contents.end(), 
                 [](const auto& a, const auto& b) { return a.second > b.second; });
        
        std::vector<uint32_t> result;
        for (uint32_t i = 0; i < std::min(limit, static_cast<uint32_t>(contents.size())); ++i) {
            result.push_back(contents[i].first);
        }
        return result;
    }
};

// ============================================================================
// Computational Task
// ============================================================================

struct ComputationalTask {
    uint32_t taskId;
    uint32_t userId;
    uint32_t complexity;  // CPU cycles required
    uint32_t dataSize;    // Data size in bytes
    Time deadline;
    Time arrivalTime;
    bool isCompleted;
    
    ComputationalTask(uint32_t id, uint32_t user, uint32_t comp, uint32_t data) 
        : taskId(id), userId(user), complexity(comp), dataSize(data), 
          isCompleted(false) {
        arrivalTime = Simulator::Now();
        deadline = arrivalTime + Seconds(5.0); // 5 second deadline
    }
};

// ============================================================================
// Edge Node with Computational Capacity
// ============================================================================

class EdgeNode {
private:
    uint32_t nodeId;
    std::string nodeType;  // "MEC", "FOG", "CDN"
    LRUCache cache;
    uint32_t maxConcurrentTasks;
    uint32_t currentTasks;
    std::queue<ComputationalTask> taskQueue;
    uint32_t totalTasksProcessed;
    uint32_t totalTasksDropped;
    uint64_t totalProcessingTime;
    
    // Network metrics
    uint64_t totalRequests;
    uint64_t cacheHits;
    uint64_t cacheMisses;
    uint64_t localProcessing;
    uint64_t forwardedRequests;
    
public:
    EdgeNode(uint32_t id, const std::string& type, uint64_t cacheSize, uint32_t maxTasks)
        : nodeId(id), nodeType(type), cache(cacheSize), maxConcurrentTasks(maxTasks),
          currentTasks(0), totalTasksProcessed(0), totalTasksDropped(0), totalProcessingTime(0),
          totalRequests(0), cacheHits(0), cacheMisses(0), localProcessing(0), forwardedRequests(0) {}
    
    bool processRequest(uint32_t contentId, uint64_t contentSize, uint32_t taskComplexity = 0) {
        totalRequests++;
        
        // Check cache first
        if (cache.get(contentId)) {
            cacheHits++;
            return true;
        }
        
        cacheMisses++;
        
        // If it's a computational task
        if (taskComplexity > 0) {
            if (currentTasks < maxConcurrentTasks) {
                currentTasks++;
                localProcessing++;
                
                // Simulate processing time based on complexity
                Time processingTime = MicroSeconds(taskComplexity * 100); // 100μs per complexity unit
                totalProcessingTime += processingTime.GetMicroSeconds();
                
                // Add to cache if popular enough
                if (taskComplexity > 1000) { // High complexity tasks are cached
                    cache.put(contentId, contentSize);
                }
                
                // Schedule task completion
                Simulator::Schedule(processingTime, &EdgeNode::taskCompleted, this);
                return true;
            } else {
                // Queue is full, forward to fallback
                forwardedRequests++;
                return false;
            }
        } else {
            // Content request - add to cache based on popularity
            cache.put(contentId, contentSize);
            return false; // Need to fetch from upstream
        }
    }
    
    void taskCompleted() {
        currentTasks--;
        totalTasksProcessed++;
        
        // Process next task in queue if any
        if (!taskQueue.empty() && currentTasks < maxConcurrentTasks) {
            auto task = taskQueue.front();
            taskQueue.pop();
            processRequest(task.taskId, task.dataSize, task.complexity);
        }
    }
    
    // Getters for metrics
    uint64_t getTotalRequests() const { return totalRequests; }
    uint64_t getCacheHits() const { return cacheHits; }
    uint64_t getCacheMisses() const { return cacheMisses; }
    uint64_t getLocalProcessing() const { return localProcessing; }
    uint64_t getForwardedRequests() const { return forwardedRequests; }
    uint32_t getTotalTasksProcessed() const { return totalTasksProcessed; }
    uint32_t getTotalTasksDropped() const { return totalTasksDropped; }
    double getCacheHitRatio() const { 
        return totalRequests > 0 ? static_cast<double>(cacheHits) / totalRequests : 0.0; 
    }
    double getAverageProcessingTime() const {
        return totalTasksProcessed > 0 ? static_cast<double>(totalProcessingTime) / totalTasksProcessed : 0.0;
    }
    uint64_t getCacheSize() const { return cache.getSize(); }
    uint64_t getCacheMaxSize() const { return cache.getMaxSize(); }
    size_t getCacheEntries() const { return cache.getEntryCount(); }
    std::string getNodeType() const { return nodeType; }
    uint32_t getNodeId() const { return nodeId; }
};

// ============================================================================
// Global Metrics Collection
// ============================================================================

struct GlobalMetrics {
    uint64_t totalRequests;
    uint64_t totalCacheHits;
    uint64_t totalLocalProcessing;
    uint64_t totalForwarded;
    uint64_t totalLatency;
    uint32_t totalTasksProcessed;
    uint32_t totalTasksDropped;
    
    std::vector<Time> latencies;
    std::vector<double> cacheHitRatios;
    std::vector<double> processingTimes;
    
    GlobalMetrics() : totalRequests(0), totalCacheHits(0), totalLocalProcessing(0),
                     totalForwarded(0), totalLatency(0), totalTasksProcessed(0), totalTasksDropped(0) {}
};

static GlobalMetrics g_metrics;
static std::vector<std::unique_ptr<EdgeNode>> g_edgeNodes;

// ============================================================================
// User Application
// ============================================================================

class UserApp : public Application {
private:
    uint32_t userId;
    uint32_t totalRequests;
    uint32_t requestsSent;
    Ptr<Socket> socket;
    std::vector<Ptr<Node>> edgeNodes;
    std::vector<Ptr<Node>> cdnNodes;
    
    // Request patterns - natural arrival times
    std::uniform_int_distribution<uint32_t> contentDist;
    std::uniform_int_distribution<uint32_t> complexityDist;
    std::exponential_distribution<double> timeDist;
    std::mt19937 rng;
    
public:
    UserApp(uint32_t id, uint32_t requests, 
            const std::vector<Ptr<Node>>& edges, const std::vector<Ptr<Node>>& cdns)
        : userId(id), totalRequests(requests), requestsSent(0),
          edgeNodes(edges), cdnNodes(cdns), contentDist(1, 1000), complexityDist(100, 5000),
          timeDist(0.1), rng(id) { // Natural arrival rate
        socket = Socket::CreateSocket(GetNode(), TypeId::LookupByName("ns3::UdpSocketFactory"));
    }
    
    void StartApplication() override {
        ScheduleNextRequest();
    }
    
    void StopApplication() override {
        if (socket) {
            socket->Close();
        }
    }
    
private:
    void ScheduleNextRequest() {
        if (requestsSent < totalRequests) {
            // Natural request arrival - exponential distribution with mean 0.1 seconds
            double nextInterval = timeDist(rng);
            Simulator::Schedule(Seconds(nextInterval), &UserApp::SendRequest, this);
        }
    }
    
    void SendRequest() {
        Time startTime = Simulator::Now();
        
        uint32_t contentId = contentDist(rng);
        uint32_t complexity = complexityDist(rng);
        uint64_t contentSize = 1024 + (contentId % 10000) * 1024; // 1KB to 10MB
        
        // Determine target node based on scenario
        Ptr<Node> targetNode = SelectTargetNode();
        
        // Simulate network latency
        Time latency = SimulateNetworkLatency(targetNode);
        
        // Process request at target node
        bool processed = ProcessRequestAtNode(targetNode, contentId, contentSize, complexity);
        
        // Update metrics
        g_metrics.totalRequests++;
        g_metrics.totalLatency += latency.GetMicroSeconds();
        g_metrics.latencies.push_back(latency);
        
        if (processed) {
            g_metrics.totalLocalProcessing++;
        } else {
            g_metrics.totalForwarded++;
        }
        
        requestsSent++;
        ScheduleNextRequest();
    }
    
    Ptr<Node> SelectTargetNode() {
        // Simple round-robin selection for now
        // In reality, this would be based on load balancing, proximity, etc.
        if (!edgeNodes.empty()) {
            return edgeNodes[requestsSent % edgeNodes.size()];
        } else {
            return cdnNodes[requestsSent % cdnNodes.size()];
        }
    }
    
    Time SimulateNetworkLatency(Ptr<Node> targetNode) {
        // Simulate realistic network latency based on distance and network conditions
        double baseLatency = 1.0; // 1ms base latency
        double distanceFactor = 1.0 + (requestsSent % 100) / 1000.0; // 0-10% variation
        double congestionFactor = 1.0 + (requestsSent % 50) / 1000.0; // 0-5% congestion
        
        double totalLatency = baseLatency * distanceFactor * congestionFactor;
        return MilliSeconds(totalLatency);
    }
    
    bool ProcessRequestAtNode(Ptr<Node> targetNode, uint32_t contentId, uint64_t contentSize, uint32_t complexity) {
        // Find corresponding edge node
        for (auto& edgeNode : g_edgeNodes) {
            if (edgeNode->getNodeId() == targetNode->GetId()) {
                return edgeNode->processRequest(contentId, contentSize, complexity);
            }
        }
        return false;
    }
};

// ============================================================================
// Main Simulation
// ============================================================================

int main(int argc, char** argv) {
    uint32_t scenario = 1;
    std::string tag = "realistic";
    std::string logDir = "results";
    uint32_t numUsers = 1000;
    uint32_t requestsPerUser = 10; // 10,000 total requests
    uint32_t numMecNodes = 10;
    uint32_t numFogNodes = 20;
    uint32_t numCdnNodes = 5;
    
    // Cache configurations
    uint32_t mecCacheMB = 512;
    uint32_t fogCacheMB = 256;
    uint32_t cdnCacheMB = 2048;
    
    // Computational capacity
    uint32_t mecMaxTasks = 100;
    uint32_t fogMaxTasks = 50;
    uint32_t cdnMaxTasks = 200;
    
    CommandLine cmd(__FILE__);
    cmd.AddValue("scenario", "Scenario number", scenario);
    cmd.AddValue("runTag", "Run tag for logging", tag);
    cmd.AddValue("logDir", "Directory to write CSV logs", logDir);
    cmd.AddValue("users", "Number of users", numUsers);
    cmd.AddValue("requestsPerUser", "Requests per user", requestsPerUser);
    cmd.AddValue("mecNodes", "Number of MEC nodes", numMecNodes);
    cmd.AddValue("fogNodes", "Number of FOG nodes", numFogNodes);
    cmd.AddValue("cdnNodes", "Number of CDN nodes", numCdnNodes);
    cmd.AddValue("mecCacheMB", "MEC cache size in MB", mecCacheMB);
    cmd.AddValue("fogCacheMB", "FOG cache size in MB", fogCacheMB);
    cmd.AddValue("cdnCacheMB", "CDN cache size in MB", cdnCacheMB);
    cmd.AddValue("mecMaxTasks", "MEC max concurrent tasks", mecMaxTasks);
    cmd.AddValue("fogMaxTasks", "FOG max concurrent tasks", fogMaxTasks);
    cmd.AddValue("cdnMaxTasks", "CDN max concurrent tasks", cdnMaxTasks);
    cmd.Parse(argc, argv);

    Time::SetResolution(Time::NS);
    
    // Create output directory
    std::filesystem::create_directories(logDir + "/" + tag);
    
    // Create nodes
    NodeContainer userNodes, mecNodes, fogNodes, cdnNodes;
    userNodes.Create(numUsers);
    mecNodes.Create(numMecNodes);
    fogNodes.Create(numFogNodes);
    cdnNodes.Create(numCdnNodes);
    
    // Create edge nodes
    g_edgeNodes.clear();
    
    // MEC nodes
    for (uint32_t i = 0; i < numMecNodes; ++i) {
        g_edgeNodes.emplace_back(std::make_unique<EdgeNode>(
            mecNodes.Get(i)->GetId(), "MEC", 
            static_cast<uint64_t>(mecCacheMB) * 1024 * 1024, mecMaxTasks));
    }
    
    // FOG nodes
    for (uint32_t i = 0; i < numFogNodes; ++i) {
        g_edgeNodes.emplace_back(std::make_unique<EdgeNode>(
            fogNodes.Get(i)->GetId(), "FOG", 
            static_cast<uint64_t>(fogCacheMB) * 1024 * 1024, fogMaxTasks));
    }
    
    // CDN nodes
    for (uint32_t i = 0; i < numCdnNodes; ++i) {
        g_edgeNodes.emplace_back(std::make_unique<EdgeNode>(
            cdnNodes.Get(i)->GetId(), "CDN", 
            static_cast<uint64_t>(cdnCacheMB) * 1024 * 1024, cdnMaxTasks));
    }
    
    // Create network
    InternetStackHelper internet;
    internet.Install(NodeContainer(userNodes, mecNodes, fogNodes, cdnNodes));
    
    // Create point-to-point links
    PointToPointHelper p2p;
    p2p.SetDeviceAttribute("DataRate", StringValue("100Mbps"));
    p2p.SetChannelAttribute("Delay", StringValue("2ms"));
    
    // Connect users to edge nodes based on scenario
    std::vector<NetDeviceContainer> userLinks;
    std::vector<Ptr<Node>> edgeNodePtrs, cdnNodePtrs;
    
    for (uint32_t i = 0; i < numMecNodes; ++i) {
        edgeNodePtrs.push_back(mecNodes.Get(i));
    }
    for (uint32_t i = 0; i < numFogNodes; ++i) {
        edgeNodePtrs.push_back(fogNodes.Get(i));
    }
    for (uint32_t i = 0; i < numCdnNodes; ++i) {
        cdnNodePtrs.push_back(cdnNodes.Get(i));
    }
    
    // Connect users to edge nodes
    for (uint32_t i = 0; i < numUsers; ++i) {
        Ptr<Node> targetNode = edgeNodePtrs[i % edgeNodePtrs.size()];
        NetDeviceContainer link = p2p.Install(userNodes.Get(i), targetNode);
        userLinks.push_back(link);
    }
    
    // Assign IP addresses
    Ipv4AddressHelper ipv4;
    ipv4.SetBase("10.1.0.0", "255.255.0.0");
    
    for (auto& link : userLinks) {
        ipv4.Assign(link);
        ipv4.NewNetwork();
    }
    
    // Create applications
    ApplicationContainer apps;
    // No time constraints - let requests resolve naturally
    for (uint32_t i = 0; i < numUsers; ++i) {
        Ptr<UserApp> app = CreateObject<UserApp>(i, requestsPerUser, 
                                               edgeNodePtrs, cdnNodePtrs);
        userNodes.Get(i)->AddApplication(app);
        apps.Add(app);
    }
    
    // Enable flow monitoring
    FlowMonitorHelper flowmon;
    Ptr<FlowMonitor> monitor = flowmon.InstallAll();
    
    // Start simulation - no time constraints
    apps.Start(Seconds(1.0));
    
    // Stop when all requests are processed
    Simulator::Stop(Seconds(3600.0)); // 1 hour maximum as safety
    Simulator::Run();
    
    // Collect final metrics
    monitor->CheckForLostPackets();
    Ptr<Ipv4FlowClassifier> classifier = DynamicCast<Ipv4FlowClassifier>(flowmon.GetClassifier());
    FlowMonitor::FlowStatsContainer stats = monitor->GetFlowStats();
    
    // Calculate network metrics
    uint64_t totalPackets = 0;
    uint64_t lostPackets = 0;
    uint64_t totalDelay = 0;
    uint64_t totalThroughput = 0;
    
    for (auto& stat : stats) {
        totalPackets += stat.second.rxPackets + stat.second.lostPackets;
        lostPackets += stat.second.lostPackets;
        totalDelay += stat.second.delaySum.GetMicroSeconds();
        totalThroughput += stat.second.rxBytes * 8; // Convert to bits
    }
    
    double packetLossRate = totalPackets > 0 ? static_cast<double>(lostPackets) / totalPackets : 0.0;
    double averageDelay = stats.size() > 0 ? static_cast<double>(totalDelay) / stats.size() : 0.0;
    double totalThroughputMbps = static_cast<double>(totalThroughput) / 1000000.0 / duration;
    
    // Calculate latency statistics
    std::sort(g_metrics.latencies.begin(), g_metrics.latencies.end());
    double p50Latency = g_metrics.latencies.size() > 0 ? 
        g_metrics.latencies[g_metrics.latencies.size() / 2].GetMicroSeconds() : 0.0;
    double p95Latency = g_metrics.latencies.size() > 0 ? 
        g_metrics.latencies[static_cast<size_t>(g_metrics.latencies.size() * 0.95)].GetMicroSeconds() : 0.0;
    double p99Latency = g_metrics.latencies.size() > 0 ? 
        g_metrics.latencies[static_cast<size_t>(g_metrics.latencies.size() * 0.99)].GetMicroSeconds() : 0.0;
    
    // Write summary file
    std::ofstream summary(logDir + "/" + tag + "/summary.csv");
    summary << "tag,scenario,req,served,local,peer,up,hL,hP,hU,latency_p50,latency_p95,latency_p99,packet_loss,throughput_mbps,tasks_processed,tasks_dropped\n";
    
    uint64_t totalServed = g_metrics.totalLocalProcessing + g_metrics.totalForwarded;
    double localRatio = g_metrics.totalRequests > 0 ? static_cast<double>(g_metrics.totalLocalProcessing) / g_metrics.totalRequests : 0.0;
    double peerRatio = 0.0; // Will be calculated based on P2P sharing
    double upstreamRatio = g_metrics.totalRequests > 0 ? static_cast<double>(g_metrics.totalForwarded) / g_metrics.totalRequests : 0.0;
    
    summary << tag << "," << scenario << "," 
            << g_metrics.totalRequests << "," << totalServed << ","
            << g_metrics.totalLocalProcessing << "," << 0 << "," << g_metrics.totalForwarded << ","
            << localRatio << "," << peerRatio << "," << upstreamRatio << ","
            << p50Latency << "," << p95Latency << "," << p99Latency << ","
            << packetLossRate << "," << totalThroughputMbps << ","
            << g_metrics.totalTasksProcessed << "," << g_metrics.totalTasksDropped << "\n";
    summary.close();
    
    // Write detailed node metrics
    std::ofstream nodes_file(logDir + "/" + tag + "/nodes.csv");
    nodes_file << "tag,scenario,node,label,role,req,local,peer,up,cacheSz,cacheCap,cacheEntries,hitRatio,avgProcessingTime,tasksProcessed,tasksDropped\n";
    
    uint32_t nodeIndex = 0;
    for (const auto& node : g_edgeNodes) {
        nodes_file << tag << "," << scenario << "," << nodeIndex++ << ","
                  << node->getNodeType() << "-" << node->getNodeId() << ","
                  << node->getNodeType() << ","
                  << node->getTotalRequests() << ","
                  << node->getLocalProcessing() << ","
                  << 0 << "," // peer hits (will be implemented for P2P)
                  << node->getForwardedRequests() << ","
                  << node->getCacheSize() << ","
                  << node->getCacheMaxSize() << ","
                  << node->getCacheEntries() << ","
                  << node->getCacheHitRatio() << ","
                  << node->getAverageProcessingTime() << ","
                  << node->getTotalTasksProcessed() << ","
                  << node->getTotalTasksDropped() << "\n";
    }
    nodes_file.close();
    
    // Write network metrics
    std::ofstream network_file(logDir + "/" + tag + "/network_metrics.csv");
    network_file << "tag,scenario,avg_latency_ms,p50_latency_ms,p95_latency_ms,p99_latency_ms,packet_loss_rate,throughput_mbps,total_packets,lost_packets\n";
    network_file << tag << "," << scenario << ","
                << averageDelay / 1000.0 << "," // Convert to ms
                << p50Latency / 1000.0 << ","
                << p95Latency / 1000.0 << ","
                << p99Latency / 1000.0 << ","
                << packetLossRate << ","
                << totalThroughputMbps << ","
                << totalPackets << ","
                << lostPackets << "\n";
    network_file.close();
    
    std::cout << "Generated realistic simulation results for tag: " << tag << std::endl;
    std::cout << "Total Requests: " << g_metrics.totalRequests << std::endl;
    std::cout << "Cache Hit Ratio: " << (g_metrics.totalRequests > 0 ? static_cast<double>(g_metrics.totalCacheHits) / g_metrics.totalRequests : 0.0) << std::endl;
    std::cout << "Average Latency: " << averageDelay / 1000.0 << " ms" << std::endl;
    std::cout << "Packet Loss Rate: " << packetLossRate * 100 << "%" << std::endl;
    std::cout << "Throughput: " << totalThroughputMbps << " Mbps" << std::endl;
    std::cout << "Tasks Processed: " << g_metrics.totalTasksProcessed << std::endl;
    std::cout << "Tasks Dropped: " << g_metrics.totalTasksDropped << std::endl;

    Simulator::Destroy();
    return 0;
}