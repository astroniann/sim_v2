/*
 * Tiered Edge Computing Simulation with Smart Caching
 * Implements four models: Traditional CDN, MEC-CDN, Fog-CDN, Hybrid MEC-Fog-CDN
 */

#include "ns3/core-module.h"
#include "ns3/network-module.h" 
#include "ns3/internet-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/applications-module.h"
#include "ns3/mobility-module.h"
#include "ns3/csma-module.h"
#include "ns3/point-to-point-layout-module.h"
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <unordered_map>
#include <random>
#include <algorithm>
#include <memory>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("TieredEdgeSimulation");

// Forward declarations
class SmartEdgeNode;
class RealisticTrafficApp;

// Enums and data structures
enum ContentCategory {
    VIDEO_STREAMING,
    GAMING,
    AUGMENTED_REALITY,
    WEB_BROWSING,
    SOCIAL_MEDIA
};

struct ContentMetadata {
    ContentCategory category;
    uint32_t size;
    double popularity;
    std::vector<std::string> relatedContent;
    uint32_t accessCount;
    Time lastAccess;
};

// Simple LRU Cache with Popularity Tracking
template<typename Key, typename Value>
class PopularityLRUCache {
private:
    struct Node {
        Key key;
        Value value;
        double popularity;
        std::shared_ptr<Node> prev, next;
        
        Node(Key k, Value v, double p = 0.0) : key(k), value(v), popularity(p) {}
    };
    
    std::unordered_map<Key, std::shared_ptr<Node>> cache;
    std::shared_ptr<Node> head, tail;
    size_t capacity;
    
public:
    PopularityLRUCache(size_t cap) : capacity(cap) {
        head = std::make_shared<Node>(Key{}, Value{});
        tail = std::make_shared<Node>(Key{}, Value{});
        head->next = tail;
        tail->prev = head;
    }
    
    bool Contains(const Key& key) {
        return cache.find(key) != cache.end();
    }
    
    Value Get(const Key& key) {
        auto it = cache.find(key);
        if (it != cache.end()) {
            moveToHead(it->second);
            return it->second->value;
        }
        return Value{};
    }
    
    void Insert(const Key& key, const Value& value, double popularity = 0.0) {
        auto it = cache.find(key);
        if (it != cache.end()) {
            it->second->value = value;
            it->second->popularity = popularity;
            moveToHead(it->second);
            return;
        }
        
        if (cache.size() >= capacity) {
            removeTail();
        }
        
        auto newNode = std::make_shared<Node>(key, value, popularity);
        cache[key] = newNode;
        addToHead(newNode);
    }
    
    size_t Size() const { return cache.size(); }
    
private:
    void addToHead(std::shared_ptr<Node> node) {
        node->prev = head;
        node->next = head->next;
        head->next->prev = node;
        head->next = node;
    }
    
    void removeNode(std::shared_ptr<Node> node) {
        node->prev->next = node->next;
        node->next->prev = node->prev;
    }
    
    void moveToHead(std::shared_ptr<Node> node) {
        removeNode(node);
        addToHead(node);
    }
    
    void removeTail() {
        auto last = tail->prev;
        removeNode(last);
        cache.erase(last->key);
    }
};

// Popularity Tracker
class PopularityTracker {
private:
    std::unordered_map<std::string, ContentMetadata> contentMap;
    std::random_device rd;
    std::mt19937 gen;
    
public:
    PopularityTracker() : gen(rd()) {}
    
    void RegisterContent(const std::string& content, ContentCategory category, 
                        uint32_t size, double initialPopularity, 
                        const std::vector<std::string>& related) {
        contentMap[content] = {category, size, initialPopularity, related, 0, Seconds(0)};
    }
    
    void RecordAccess(const std::string& content) {
        auto it = contentMap.find(content);
        if (it != contentMap.end()) {
            it->second.accessCount++;
            it->second.lastAccess = Simulator::Now();
            // Update popularity based on recency and frequency
            double timeFactor = 1.0 / (1.0 + (Simulator::Now().GetSeconds() - it->second.lastAccess.GetSeconds()));
            it->second.popularity = 0.7 * it->second.popularity + 0.3 * (it->second.accessCount * timeFactor);
        }
    }
    
    ContentMetadata GetContentInfo(const std::string& content) {
        auto it = contentMap.find(content);
        if (it != contentMap.end()) {
            return it->second;
        }
        return {WEB_BROWSING, 1024, 0.1, {}, 0, Seconds(0)};
    }
    
    double GetPopularity(const std::string& content) {
        auto it = contentMap.find(content);
        return (it != contentMap.end()) ? it->second.popularity : 0.1;
    }
};

// P2P Cache Manager
class P2PCacheManager {
private:
    PopularityTracker* tracker;
    std::vector<Address> peers;
    
public:
    P2PCacheManager(PopularityTracker* t) : tracker(t) {}
    
    void AddPeer(const Address& peer) {
        peers.push_back(peer);
    }
    
    void SharePopularityUpdate(const std::string& content, double popularity) {
        // Simulate P2P popularity sharing
        NS_LOG_DEBUG("Sharing popularity update for " << content << ": " << popularity);
    }
    
    std::vector<Address> GetRecommendedPeers(const std::string& content) {
        // Return subset of peers that might have the content
        std::vector<Address> recommended;
        size_t maxPeers = std::min(peers.size(), size_t(3));
        for (size_t i = 0; i < maxPeers; i++) {
            recommended.push_back(peers[i]);
        }
        return recommended;
    }
};

// Smart Edge Node Class
class SmartEdgeNode : public Application {
private:
    Ptr<Socket> m_socket;
    Address m_local;
    std::string m_nodeType;
    uint32_t m_cacheSize;
    uint32_t m_computeCapacity;
    std::vector<Address> m_peers;
    
    // Smart components
    std::unique_ptr<PopularityLRUCache<std::string, bool>> m_cache;
    std::unique_ptr<PopularityTracker> m_tracker;
    std::unique_ptr<P2PCacheManager> m_p2pManager;
    
    // Statistics
    uint32_t m_totalRequests;
    uint32_t m_cacheHits;
    uint32_t m_peerHits;
    uint32_t m_cdnRequests;
    uint32_t m_edgeComputations;
    double m_totalLatency;
    
    ContentCategory ClassifyContent(const std::string& content);
    
public:
    SmartEdgeNode();
    virtual ~SmartEdgeNode();
    
    void Setup(Address address, std::string nodeType, uint32_t cacheSize, 
               uint32_t computeCapacity, std::vector<Address> peers);
    void InitializePopularContent();
    
protected:
    virtual void StartApplication(void);
    virtual void StopApplication(void);
    
private:
    void HandleRead(Ptr<Socket> socket);
    void ProcessRequest(const std::string& content, Address clientAddr);
    void QueryPeers(const std::string& content, Address clientAddr);
    void HandlePeerResponse(const std::string& content, Address clientAddr);
    void RequestFromCDN(const std::string& content, Address clientAddr);
    void ComputeAndCache(const std::string& content, Address clientAddr);
    double CalculatePopularity(const std::string& content);
    void SendResponse(const std::string& response, Address clientAddr);
    
    // Statistics methods
public:
    uint32_t GetTotalRequests() const { return m_totalRequests; }
    double GetAverageLatency() const { 
        return m_totalRequests > 0 ? m_totalLatency / m_totalRequests : 0.0; 
    }
    double GetCacheHitRatio() const { 
        return m_totalRequests > 0 ? (double)m_cacheHits / m_totalRequests * 100.0 : 0.0; 
    }
    double GetPeerHitRatio() const { 
        return m_totalRequests > 0 ? (double)m_peerHits / m_totalRequests * 100.0 : 0.0; 
    }
    double GetCDNOffloadRatio() const { 
        return m_totalRequests > 0 ? (1.0 - (double)m_cdnRequests / m_totalRequests) * 100.0 : 0.0; 
    }
    uint32_t GetEdgeComputations() const { return m_edgeComputations; }
    uint32_t GetCDNRequests() const { return m_cdnRequests; }
};

// SmartEdgeNode Implementation
SmartEdgeNode::SmartEdgeNode() 
    : m_totalRequests(0), m_cacheHits(0), m_peerHits(0), 
      m_cdnRequests(0), m_edgeComputations(0), m_totalLatency(0.0) {
}

SmartEdgeNode::~SmartEdgeNode() {
    m_socket = 0;
}

void SmartEdgeNode::Setup(Address address, std::string nodeType, uint32_t cacheSize, 
                         uint32_t computeCapacity, std::vector<Address> peers) {
    m_local = address;
    m_nodeType = nodeType;
    m_cacheSize = cacheSize;
    m_computeCapacity = computeCapacity;
    m_peers = peers;
    
    // Initialize smart components
    m_tracker = std::make_unique<PopularityTracker>();
    m_cache = std::make_unique<PopularityLRUCache<std::string, bool>>(cacheSize);
    m_p2pManager = std::make_unique<P2PCacheManager>(m_tracker.get());
    
    // Add peers to P2P manager
    for (const auto& peer : peers) {
        m_p2pManager->AddPeer(peer);
    }
}

void SmartEdgeNode::InitializePopularContent() {
    // Pre-populate cache with popular content based on node type
    std::vector<std::string> popularContent;
    
    if (m_nodeType == "MEC" || m_nodeType == "Fog") {
        popularContent = {"video1.mp4", "game_asset_1", "ar_model_1", 
                         "social_feed_1", "web_page_1"};
    } else {
        popularContent = {"popular_video.mp4", "trending_game", "viral_content"};
    }
    
    for (const auto& content : popularContent) {
        ContentCategory category = ClassifyContent(content);
        m_tracker->RegisterContent(content, category, 1400, 0.5, {content});
        m_cache->Insert(content, true);
    }
}

void SmartEdgeNode::StartApplication(void) {
    if (!m_socket) {
        TypeId tid = TypeId::LookupByName("ns3::UdpSocketFactory");
        m_socket = Socket::CreateSocket(GetNode(), tid);
        m_socket->Bind(m_local);
    }
    
    m_socket->SetRecvCallback(MakeCallback(&SmartEdgeNode::HandleRead, this));
    InitializePopularContent();
}

void SmartEdgeNode::StopApplication(void) {
    if (m_socket) {
        m_socket->Close();
        m_socket->SetRecvCallback(MakeNullCallback<void, Ptr<Socket>>());
    }
}

void SmartEdgeNode::HandleRead(Ptr<Socket> socket) {
    Ptr<Packet> packet;
    Address from;
    while ((packet = socket->RecvFrom(from))) {
        if (packet->GetSize() == 0) {
            break;
        }
        
        // Extract request content (simplified)
        uint8_t data[256];
        packet->CopyData(data, std::min(packet->GetSize(), (uint32_t)255));
        data[std::min(packet->GetSize(), (uint32_t)255)] = '\0';
        std::string requestedContent(reinterpret_cast<char*>(data));
        
        ProcessRequest(requestedContent, from);
    }
}

void SmartEdgeNode::ProcessRequest(const std::string& content, Address clientAddr) {
    m_totalRequests++;
    m_tracker->RecordAccess(content);
    
    // Check local cache first
    if (m_cache->Contains(content)) {
        m_cacheHits++;
        m_totalLatency += 1.0; // 1ms cache hit latency
        SendResponse("CACHE_HIT: " + content, clientAddr);
        return;
    }
    
    // For MEC/Fog nodes, try P2P sharing
    if (m_nodeType == "MEC" || m_nodeType == "Fog") {
        QueryPeers(content, clientAddr);
    } else {
        // Traditional CDN - direct CDN request
        RequestFromCDN(content, clientAddr);
    }
}

void SmartEdgeNode::QueryPeers(const std::string& content, Address clientAddr) {
    auto recommendedPeers = m_p2pManager->GetRecommendedPeers(content);
    
    if (!recommendedPeers.empty()) {
        // Simulate peer query with 60% success rate
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(0.0, 1.0);
        
        if (dis(gen) < 0.6) {
            // Peer hit
            Simulator::Schedule(MilliSeconds(5), &SmartEdgeNode::HandlePeerResponse, 
                              this, content, clientAddr);
            return;
        }
    }
    
    // No peer hit, check if we can compute locally
    if (m_computeCapacity > 0) {
        ComputeAndCache(content, clientAddr);
    } else {
        RequestFromCDN(content, clientAddr);
    }
}

void SmartEdgeNode::HandlePeerResponse(const std::string& content, Address clientAddr) {
    m_peerHits++;
    m_totalLatency += 5.0; // 5ms peer sharing latency
    
    // Cache the content locally for future use
    if (m_cache) {
        m_cache->Insert(content, true);
    }
    
    SendResponse("PEER_HIT: " + content, clientAddr);
}

void SmartEdgeNode::RequestFromCDN(const std::string& content, Address clientAddr) {
    m_cdnRequests++;
    
    // Simulate CDN latency based on network conditions
    double cdnLatency = 50.0; // Base 50ms CDN latency
    
    // Add network congestion factor
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.8, 1.5);
    cdnLatency *= dis(gen);
    
    m_totalLatency += cdnLatency;
    
    Simulator::Schedule(MilliSeconds(static_cast<uint64_t>(cdnLatency)), 
                       &SmartEdgeNode::SendResponse, this, "CDN_RESPONSE: " + content, clientAddr);
    
    // Cache popular content
    double popularity = CalculatePopularity(content);
    if (popularity > 0.3 && m_cache) {
        m_cache->Insert(content, true, popularity);
    }
}

void SmartEdgeNode::ComputeAndCache(const std::string& content, Address clientAddr) {
    m_edgeComputations++;
    
    ContentCategory category = ClassifyContent(content);
    double computeTime = 10.0; // Base computation time
    
    switch (category) {
        case VIDEO_STREAMING:
            computeTime = 15.0;
            break;
        case GAMING:
            computeTime = 8.0;
            break;
        case AUGMENTED_REALITY:
            computeTime = 20.0;
            break;
        default:
            computeTime = 10.0;
    }
    
    m_totalLatency += computeTime;
    
    // Schedule computation completion
    Simulator::Schedule(MilliSeconds(static_cast<uint64_t>(computeTime)), 
                       [this, content, clientAddr]() {
        // Cache the computed result
        if (m_cache) {
            m_cache->Insert(content, true);
        }
        
        SendResponse("COMPUTED: " + content, clientAddr);
        
        // Update popularity and share with peers
        double popularity = CalculatePopularity(content);
        if (m_p2pManager) {
            m_p2pManager->SharePopularityUpdate(content, popularity);
        }
    });
}

ContentCategory SmartEdgeNode::ClassifyContent(const std::string& content) {
    if (content.find("video") != std::string::npos || content.find(".mp4") != std::string::npos) {
        return VIDEO_STREAMING;
    } else if (content.find("game") != std::string::npos) {
        return GAMING;
    } else if (content.find("ar_") != std::string::npos) {
        return AUGMENTED_REALITY;
    } else if (content.find("social") != std::string::npos) {
        return SOCIAL_MEDIA;
    }
    return WEB_BROWSING;
}

double SmartEdgeNode::CalculatePopularity(const std::string& content) {
    if (m_tracker) {
        auto metadata = m_tracker->GetContentInfo(content);
        return metadata.popularity;
    }
    return 0.1; // Default low popularity
}

void SmartEdgeNode::SendResponse(const std::string& response, Address clientAddr) {
    if (m_socket) {
        Ptr<Packet> packet = Create<Packet>((uint8_t*)response.c_str(), response.length());
        m_socket->SendTo(packet, 0, clientAddr);
    }
}

// Realistic Traffic Application
class RealisticTrafficApp : public Application {
private:
    Ptr<Socket> m_socket;
    Address m_peer;
    std::vector<std::string> m_contentPool;
    std::discrete_distribution<int> m_zipfDistribution;
    std::mt19937 m_generator;
    EventId m_sendEvent;
    uint32_t m_requestsSent;
    uint32_t m_maxRequests;
    
public:
    RealisticTrafficApp();
    virtual ~RealisticTrafficApp();
    
    void Setup(Address address, const std::vector<std::string>& contentPool, 
               const std::discrete_distribution<int>& zipfDist);
    
protected:
    virtual void StartApplication(void);
    virtual void StopApplication(void);
    
private:
    void SendRequest(void);
    void ScheduleNextRequest(void);
};

RealisticTrafficApp::RealisticTrafficApp() : m_generator(std::random_device{}()), m_requestsSent(0), m_maxRequests(0) {
}

RealisticTrafficApp::~RealisticTrafficApp() {
    m_socket = 0;
}

void RealisticTrafficApp::Setup(Address address, const std::vector<std::string>& contentPool, 
                               const std::discrete_distribution<int>& zipfDist) {
    m_peer = address;
    m_contentPool = contentPool;
    m_zipfDistribution = zipfDist;
    
    // Calculate max requests per user to achieve target
    uint32_t totalUsers = g_tier1Cities * g_usersPerCity + g_tier2Cities * g_usersPerCity;
    m_maxRequests = (g_targetRequests / totalUsers) + 5; // Add buffer to ensure we hit target
}

void RealisticTrafficApp::StartApplication(void) {
    if (!m_socket) {
        TypeId tid = TypeId::LookupByName("ns3::UdpSocketFactory");
        m_socket = Socket::CreateSocket(GetNode(), tid);
    }
    
    ScheduleNextRequest();
}

void RealisticTrafficApp::StopApplication(void) {
    if (m_socket) {
        m_socket->Close();
        m_socket = 0;
    }
    
    Simulator::Cancel(m_sendEvent);
}

void RealisticTrafficApp::SendRequest(void) {
    // Check if we've reached our request limit
    if (m_requestsSent >= m_maxRequests) {
        return;
    }
    
    // Select content based on Zipf distribution
    int contentIndex = m_zipfDistribution(m_generator);
    std::string content = m_contentPool[contentIndex % m_contentPool.size()];
    
    Ptr<Packet> packet = Create<Packet>((uint8_t*)content.c_str(), content.length());
    m_socket->SendTo(packet, 0, m_peer);
    
    m_requestsSent++;
    ScheduleNextRequest();
}

void RealisticTrafficApp::ScheduleNextRequest(void) {
    // Check if we've reached our request limit
    if (m_requestsSent >= m_maxRequests) {
        return;
    }
    
    // Calculate inter-request time to achieve target request rate
    uint32_t totalUsers = g_tier1Cities * g_usersPerCity + g_tier2Cities * g_usersPerCity;
    double requestsPerUser = (double)g_targetRequests / totalUsers;
    double requestRate = requestsPerUser / g_simTime;  // requests per second per user
    
    // Use higher request rate to ensure we hit the target
    requestRate = std::max(requestRate, 1.0); // At least 1 request per second
    
    // Inter-request time follows exponential distribution
    std::exponential_distribution<> expDist(requestRate);
    double nextTime = expDist(m_generator);
    
    // Ensure we don't schedule beyond simulation time
    if (Simulator::Now().GetSeconds() + nextTime < g_simTime - 1.0) {
        m_sendEvent = Simulator::Schedule(Seconds(nextTime), &RealisticTrafficApp::SendRequest, this);
    }
}

// Global Variables
uint32_t g_model = 0;
uint32_t g_tier1Cities = 2;
uint32_t g_tier2Cities = 3;
uint32_t g_usersPerCity = 50;
uint32_t g_cacheSize = 50;
double g_simTime = 60.0;
uint32_t g_targetRequests = 10000;  // Target total number of requests

NodeContainer cdnNodes, mecNodes, fogNodes, userNodes;
std::vector<Ptr<SmartEdgeNode>> edgeNodeApps;

// Function declarations
void CreateTieredTopology();
void InstallApplications(NodeContainer& cdnNodes, NodeContainer& mecNodes, 
                        NodeContainer& fogNodes, NodeContainer& userNodes);
void InstallUserTraffic(NodeContainer& userNodes, NodeContainer& mecNodes, 
                       NodeContainer& fogNodes, NodeContainer& cdnNodes);
void PrintStatistics();
std::vector<double> GenerateZipfWeights(size_t n, double alpha = 1.0);

void CreateTieredTopology() {
    NS_LOG_INFO("Creating tiered topology with Model " << g_model);
    
    // Create nodes based on model
    switch (g_model) {
        case 0: // Traditional CDN
            cdnNodes.Create(2);
            userNodes.Create(g_tier1Cities * g_usersPerCity + g_tier2Cities * g_usersPerCity);
            break;
        case 1: // MEC-CDN
            cdnNodes.Create(1);
            mecNodes.Create(g_tier1Cities);
            userNodes.Create(g_tier1Cities * g_usersPerCity + g_tier2Cities * g_usersPerCity);
            break;
        case 2: // Fog-CDN  
            cdnNodes.Create(1);
            fogNodes.Create(g_tier2Cities);
            userNodes.Create(g_tier1Cities * g_usersPerCity + g_tier2Cities * g_usersPerCity);
            break;
        case 3: // Hybrid MEC-Fog-CDN
            cdnNodes.Create(1);
            mecNodes.Create(g_tier1Cities);
            fogNodes.Create(g_tier2Cities);
            userNodes.Create(g_tier1Cities * g_usersPerCity + g_tier2Cities * g_usersPerCity);
            break;
    }
    
    // Install Internet stack
    InternetStackHelper internet;
    internet.Install(cdnNodes);
    if (mecNodes.GetN() > 0) internet.Install(mecNodes);
    if (fogNodes.GetN() > 0) internet.Install(fogNodes);
    internet.Install(userNodes);
    
    // Create links with realistic latencies
    PointToPointHelper p2p;
    
    // CDN backbone (high capacity, moderate latency)
    p2p.SetDeviceAttribute("DataRate", StringValue("10Gbps"));
    p2p.SetChannelAttribute("Delay", StringValue("20ms"));
    
    // MEC links (high capacity, low latency)
    PointToPointHelper mecP2P;
    mecP2P.SetDeviceAttribute("DataRate", StringValue("1Gbps"));
    mecP2P.SetChannelAttribute("Delay", StringValue("2ms"));
    
    // Fog links (moderate capacity, moderate latency) 
    PointToPointHelper fogP2P;
    fogP2P.SetDeviceAttribute("DataRate", StringValue("100Mbps"));
    fogP2P.SetChannelAttribute("Delay", StringValue("5ms"));
    
    // User links (varied capacity, higher latency)
    PointToPointHelper userP2P;
    userP2P.SetDeviceAttribute("DataRate", StringValue("50Mbps"));
    userP2P.SetChannelAttribute("Delay", StringValue("10ms"));
    
    // Assign IP addresses
    Ipv4AddressHelper ipv4;
    ipv4.SetBase("10.1.0.0", "255.255.0.0");
    
    // Connect topology based on model
    // (Implementation details for network topology connections would go here)
    
    InstallApplications(cdnNodes, mecNodes, fogNodes, userNodes);
}

void InstallApplications(NodeContainer& cdnNodes, NodeContainer& mecNodes, 
                        NodeContainer& fogNodes, NodeContainer& userNodes) {
    
    // Install CDN applications
    for (uint32_t i = 0; i < cdnNodes.GetN(); i++) {
        Ptr<SmartEdgeNode> cdnApp = CreateObject<SmartEdgeNode>();
        Address addr = InetSocketAddress(Ipv4Address::GetAny(), 9000 + i);
        std::vector<Address> peers; // CDN nodes typically don't have peers
        
        cdnApp->Setup(addr, "CDN", g_cacheSize * 2, 0, peers); // CDN has larger cache, no compute
        cdnNodes.Get(i)->AddApplication(cdnApp);
        cdnApp->SetStartTime(Seconds(0.0));
        cdnApp->SetStopTime(Seconds(g_simTime));
        edgeNodeApps.push_back(cdnApp);
    }
    
    // Install MEC applications
    for (uint32_t i = 0; i < mecNodes.GetN(); i++) {
        Ptr<SmartEdgeNode> mecApp = CreateObject<SmartEdgeNode>();
        Address addr = InetSocketAddress(Ipv4Address::GetAny(), 8000 + i);
        
        // MEC nodes can peer with other MEC nodes
        std::vector<Address> peers;
        for (uint32_t j = 0; j < mecNodes.GetN(); j++) {
            if (i != j) {
                peers.push_back(InetSocketAddress(Ipv4Address::GetAny(), 8000 + j));
            }
        }
        
        mecApp->Setup(addr, "MEC", g_cacheSize, 100, peers); // High compute capacity
        mecNodes.Get(i)->AddApplication(mecApp);
        mecApp->SetStartTime(Seconds(0.0));
        mecApp->SetStopTime(Seconds(g_simTime));
        edgeNodeApps.push_back(mecApp);
    }
    
    // Install Fog applications
    for (uint32_t i = 0; i < fogNodes.GetN(); i++) {
        Ptr<SmartEdgeNode> fogApp = CreateObject<SmartEdgeNode>();
        Address addr = InetSocketAddress(Ipv4Address::GetAny(), 7000 + i);
        
        // Fog nodes can peer with other fog nodes and MEC nodes
        std::vector<Address> peers;
        for (uint32_t j = 0; j < fogNodes.GetN(); j++) {
            if (i != j) {
                peers.push_back(InetSocketAddress(Ipv4Address::GetAny(), 7000 + j));
            }
        }
        for (uint32_t j = 0; j < mecNodes.GetN(); j++) {
            peers.push_back(InetSocketAddress(Ipv4Address::GetAny(), 8000 + j));
        }
        
        fogApp->Setup(addr, "Fog", g_cacheSize, 50, peers); // Moderate compute capacity
        fogNodes.Get(i)->AddApplication(fogApp);
        fogApp->SetStartTime(Seconds(0.0));
        fogApp->SetStopTime(Seconds(g_simTime));
        edgeNodeApps.push_back(fogApp);
    }
    
    InstallUserTraffic(userNodes, mecNodes, fogNodes, cdnNodes);
}

void InstallUserTraffic(NodeContainer& userNodes, NodeContainer& mecNodes, 
                       NodeContainer& fogNodes, NodeContainer& cdnNodes) {
    
    // Content pool following realistic distribution
    std::vector<std::string> contentPool = {
        "popular_video_1.mp4", "trending_game_1", "viral_social_1",
        "popular_video_2.mp4", "trending_game_2", "viral_social_2",
        "news_article_1", "web_page_1", "ar_experience_1",
        "music_stream_1", "podcast_1", "document_1",
        "less_popular_video.mp4", "niche_game", "personal_content",
        "archive_video.mp4", "old_game", "rare_content"
    };
    
    // Generate Zipf distribution weights
    auto zipfWeights = GenerateZipfWeights(contentPool.size());
    std::discrete_distribution<int> zipf_dist(zipfWeights.begin(), zipfWeights.end());
    
    for (uint32_t u = 0; u < userNodes.GetN(); u++) {
        Ptr<Node> user = userNodes.Get(u);
        
        // Determine target server based on model and user location
        Address targetAddr;
        if (g_model == 0) {
            // Traditional CDN - all users connect to CDN
            uint32_t cdnIndex = u % cdnNodes.GetN();
            targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 9000 + cdnIndex);
        } else if (g_model == 1) {
            // MEC-CDN - tier-1 users to MEC, tier-2 to CDN
            if (u < g_tier1Cities * g_usersPerCity && mecNodes.GetN() > 0) {
                uint32_t mecIndex = (u / g_usersPerCity) % mecNodes.GetN();
                targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 8000 + mecIndex);
            } else {
                targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 9000);
            }
        } else if (g_model == 2) {
            // Fog-CDN - tier-2 users to Fog, tier-1 to CDN
            if (u >= g_tier1Cities * g_usersPerCity && fogNodes.GetN() > 0) {
                uint32_t fogIndex = ((u - g_tier1Cities * g_usersPerCity) / g_usersPerCity) % fogNodes.GetN();
                targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 7000 + fogIndex);
            } else {
                targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 9000);
            }
        } else {
            // Hybrid MEC-Fog-CDN
            if (u < g_tier1Cities * g_usersPerCity && mecNodes.GetN() > 0) {
                uint32_t mecIndex = (u / g_usersPerCity) % mecNodes.GetN();
                targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 8000 + mecIndex);
            } else if (u >= g_tier1Cities * g_usersPerCity && fogNodes.GetN() > 0) {
                uint32_t fogIndex = ((u - g_tier1Cities * g_usersPerCity) / g_usersPerCity) % fogNodes.GetN();
                targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 7000 + fogIndex);
            } else {
                targetAddr = InetSocketAddress(Ipv4Address::GetAny(), 9000);
            }
        }
        
        Ptr<RealisticTrafficApp> trafficApp = CreateObject<RealisticTrafficApp>();
        trafficApp->Setup(targetAddr, contentPool, zipf_dist);
        user->AddApplication(trafficApp);
        trafficApp->SetStartTime(Seconds(1.0 + u * 0.01));  // Staggered start
        trafficApp->SetStopTime(Seconds(g_simTime));
    }
}

std::vector<double> GenerateZipfWeights(size_t n, double alpha) {
    std::vector<double> weights(n);
    for (size_t i = 0; i < n; i++) {
        weights[i] = 1.0 / std::pow(i + 1, alpha);
    }
    return weights;
}

void PrintStatistics() {
    std::cout << "\n=== Simulation Results ===" << std::endl;
    
    uint32_t totalRequests = 0;
    double totalLatency = 0.0;
    uint32_t totalCacheHits = 0;
    uint32_t totalPeerHits = 0;
    uint32_t totalCDNRequests = 0;
    uint32_t totalEdgeComputations = 0;
    
    // Aggregate statistics from all edge nodes
    for (const auto& app : edgeNodeApps) {
        totalRequests += app->GetTotalRequests();
        totalLatency += app->GetAverageLatency() * app->GetTotalRequests();
        totalCacheHits += (app->GetCacheHitRatio() / 100.0) * app->GetTotalRequests();
        totalPeerHits += (app->GetPeerHitRatio() / 100.0) * app->GetTotalRequests();
        totalCDNRequests += app->GetCDNRequests();
        totalEdgeComputations += app->GetEdgeComputations();
    }
    
    // Calculate overall metrics
    double avgLatency = totalRequests > 0 ? totalLatency / totalRequests : 0.0;
    double cacheHitRatio = totalRequests > 0 ? (double)totalCacheHits / totalRequests * 100.0 : 0.0;
    double peerHitRatio = totalRequests > 0 ? (double)totalPeerHits / totalRequests * 100.0 : 0.0;
    double cdnOffloadRatio = totalRequests > 0 ? (1.0 - (double)totalCDNRequests / totalRequests) * 100.0 : 0.0;
    
    // Calculate total throughput (simplified)
    double throughput = totalRequests > 0 ? (totalRequests * 1.4) / g_simTime : 0.0; // Assuming 1.4 MB average content size
    
    // Print results
    std::cout << "Total Requests: " << totalRequests << std::endl;
    std::cout << "Average Latency: " << avgLatency << " ms" << std::endl;
    std::cout << "Total Throughput: " << throughput << " Mbps" << std::endl;
    std::cout << "Local Cache Hit Ratio: " << cacheHitRatio << "%" << std::endl;
    std::cout << "Peer Cache Hit Ratio: " << peerHitRatio << "%" << std::endl;
    std::cout << "CDN Offload Ratio: " << cdnOffloadRatio << "%" << std::endl;
    std::cout << "Edge Computations: " << totalEdgeComputations << std::endl;
    std::cout << "CDN Requests: " << totalCDNRequests << std::endl;
    
    // Model-specific insights
    std::string modelName;
    switch (g_model) {
        case 0: modelName = "Traditional CDN"; break;
        case 1: modelName = "MEC-CDN"; break;
        case 2: modelName = "Fog-CDN"; break;
        case 3: modelName = "Hybrid MEC-Fog-CDN"; break;
    }
    std::cout << "Model: " << modelName << std::endl;
    std::cout << "=========================\n" << std::endl;
}

int main(int argc, char *argv[]) {
    // Command line arguments
    CommandLine cmd(__FILE__);
    cmd.AddValue("model", "Simulation model (0=CDN, 1=MEC-CDN, 2=Fog-CDN, 3=Hybrid)", g_model);
    cmd.AddValue("tier1Cities", "Number of Tier-1 cities", g_tier1Cities);
    cmd.AddValue("tier2Cities", "Number of Tier-2 cities", g_tier2Cities);
    cmd.AddValue("usersPerCity", "Users per city", g_usersPerCity);
    cmd.AddValue("cacheSize", "Cache size per node", g_cacheSize);
    cmd.AddValue("simTime", "Simulation time in seconds", g_simTime);
    cmd.AddValue("targetRequests", "Target total number of requests", g_targetRequests);
    cmd.Parse(argc, argv);
    
    // Enable logging for debugging
    LogComponentEnable("TieredEdgeSimulation", LOG_LEVEL_INFO);
    
    // Validate parameters
    if (g_model > 3) {
        std::cerr << "Error: Invalid model. Use 0-3." << std::endl;
        return 1;
    }
    
    if (g_model == 1 && g_tier1Cities == 0) {
        std::cerr << "Error: MEC-CDN model requires tier1Cities > 0" << std::endl;
        return 1;
    }
    
    if (g_model == 2 && g_tier2Cities == 0) {
        std::cerr << "Error: Fog-CDN model requires tier2Cities > 0" << std::endl;
        return 1;
    }
    
    std::cout << "Starting Tiered Edge Computing Simulation" << std::endl;
    std::cout << "Model: " << g_model << ", Tier-1 Cities: " << g_tier1Cities 
              << ", Tier-2 Cities: " << g_tier2Cities << std::endl;
    std::cout << "Users per City: " << g_usersPerCity << ", Cache Size: " << g_cacheSize 
              << ", Simulation Time: " << g_simTime << "s" << std::endl;
    std::cout << "Target Requests: " << g_targetRequests << std::endl;
    
    // Create and run simulation
    CreateTieredTopology();
    
    // Enable packet tracing if needed (for detailed analysis)
    // AsciiTraceHelper ascii;
    // p2p.EnableAsciiAll(ascii.CreateFileStream("tiered-edge.tr"));
    
    // Run simulation
    Simulator::Stop(Seconds(g_simTime));
    Simulator::Run();
    
    // Print statistics
    PrintStatistics();
    
    Simulator::Destroy();
    return 0;
}
