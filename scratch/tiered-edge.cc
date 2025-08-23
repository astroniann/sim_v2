// scratch/tiered-edge.cc
// 2024-08-23 – Large-Scale MEC/FOG + CDN with P2P-gossip cache
#include "ns3/core-module.h"
#include "ns3/network-module.h"
#include "ns3/internet-module.h"
#include "ns3/applications-module.h"
#include "ns3/point-to-point-module.h"
#include "ns3/random-variable-stream.h"
#include "ns3/assert.h"

#include <algorithm>
#include <fstream>
#include <filesystem>
#include <limits>
#include <unordered_map>
#include <vector>
#include <memory>
#include <functional>
#include <initializer_list>

using namespace ns3;

NS_LOG_COMPONENT_DEFINE ("TieredEdge");

// Global metrics
struct Metrics {
    uint64_t totalRequests = 0;
    uint64_t localHits = 0;
    uint64_t peerHits = 0;
    uint64_t upstreamHits = 0;
    uint64_t queueRedirects = 0;
    uint64_t dropsEdge = 0;
    uint64_t dropsCdn = 0;
    std::vector<double> latency;
};

static Metrics g_metrics;
static std::vector<double> g_latency;

// Content information
struct ContentInfo {
    std::string id;
    uint64_t size;
    double popularity;
    uint64_t accessCount;
    
    ContentInfo(const std::string& i, uint64_t s) : id(i), size(s), popularity(1.0), accessCount(0) {}
};

// Popularity tracker with exponential decay
class PopularityTracker {
private:
    std::unordered_map<std::string, double> popularity;
    double decayRate;
    
public:
    PopularityTracker(double decay = 0.95) : decayRate(decay) {}
    
    void updatePopularity(const std::string& contentId) {
        popularity[contentId] = popularity[contentId] * decayRate + 1.0;
    }
    
    double getPopularity(const std::string& contentId) const {
        auto it = popularity.find(contentId);
        return (it != popularity.end()) ? it->second : 1.0;
    }
    
    void decayAll() {
        for (auto& pair : popularity) {
            pair.second *= decayRate;
        }
    }
};

// Popularity-aware LRU cache
class PopLRU {
private:
    struct CacheEntry {
        std::string contentId;
        uint64_t size;
        double popularity;
        uint64_t lastAccess;
        
        CacheEntry(const std::string& id, uint64_t s, double pop) 
            : contentId(id), size(s), popularity(pop), lastAccess(0) {}
    };
    
    std::list<CacheEntry> cache;
    std::unordered_map<std::string, std::list<CacheEntry>::iterator> index;
    uint64_t capacity;
    uint64_t used;
    
public:
    PopLRU(uint64_t cap) : capacity(cap), used(0) {}
    
    bool get(const std::string& contentId) {
        auto it = index.find(contentId);
        if (it != index.end()) {
            // Update access time and move to front
            it->second->lastAccess = Simulator::Now().GetMilliSeconds();
            cache.splice(cache.begin(), cache, it->second);
            return true;
        }
        return false;
    }
    
    void put(const std::string& contentId, uint64_t size, double popularity) {
        auto it = index.find(contentId);
        if (it != index.end()) {
            // Update existing entry
            it->second->popularity = popularity;
            it->second->lastAccess = Simulator::Now().GetMilliSeconds();
            cache.splice(cache.begin(), cache, it->second);
            return;
        }
        
        // Evict if necessary
        while (used + size > capacity && !cache.empty()) {
            auto last = cache.end();
            --last;
            used -= last->size;
            index.erase(last->contentId);
            cache.pop_back();
        }
        
        // Add new entry
        if (size <= capacity) {
            cache.emplace_front(contentId, size, popularity);
            index[contentId] = cache.begin();
            used += size;
        }
    }
    
    uint64_t getSize() const { return used; }
    uint64_t getCapacity() const { return capacity; }
    size_t getCount() const { return cache.size(); }
};

// P2P manager for peer-to-peer sharing
class P2PManager {
private:
    std::vector<class SmartEdgeNode*> peers;
    
public:
    void SetPeers(const std::vector<class SmartEdgeNode*>& peerList) {
        peers = peerList;
    }
    
    const std::vector<class SmartEdgeNode*>& GetPeers() const {
        return peers;
    }
};

// Smart edge node with caching and P2P capabilities
class SmartEdgeNode {
public:
    enum Role { ROLE_MEC, ROLE_FOG, ROLE_CDN };
    
private:
    Role role;
    PopLRU cache;
    PopularityTracker* tracker;
    P2PManager* p2pManager;
    Ptr<Node> node;
    std::string label;
    uint32_t maxConcurrent;
    uint32_t currentTasks;
    std::queue<std::string> requestQueue;
    
public:
    SmartEdgeNode(Role r, uint64_t cacheSize, PopularityTracker* t, P2PManager* p2p, 
                  Ptr<Node> n, const std::string& l)
        : role(r), cache(cacheSize), tracker(t), p2pManager(p2p), node(n), label(l),
          maxConcurrent(role == ROLE_MEC ? 24 : (role == ROLE_FOG ? 12 : 50)),
          currentTasks(0) {}
    
    void InitializeCatalog(const std::vector<std::string>& catalog) {
        // Pre-warm cache with popular content
        for (const auto& content : catalog) {
            if (cache.getCount() < catalog.size() / 4) {
                cache.put(content, 1024 * 1024, 1.0); // 1MB per content
            }
        }
    }
    
    void OnRequest(const std::string& contentId, SmartEdgeNode* cdnNode, bool isUserRequest) {
        if (currentTasks >= maxConcurrent) {
            // Queue overflow - redirect to CDN
            if (cdnNode) {
                cdnNode->OnRequest(contentId, nullptr, false);
            }
            g_metrics.queueRedirects++;
            return;
        }
        
        currentTasks++;
        g_metrics.totalRequests++;
        
        // Update popularity
        if (tracker) {
            tracker->updatePopularity(contentId);
        }
        
        // Check local cache first
        if (cache.get(contentId)) {
            g_metrics.localHits++;
            g_latency.push_back(role == ROLE_MEC ? 5.0 : (role == ROLE_FOG ? 8.0 : 3.0));
            currentTasks--;
            return;
        }
        
        // Check P2P peers
        if (p2pManager) {
            for (auto peer : p2pManager->GetPeers()) {
                if (peer != this && peer->cache.get(contentId)) {
                    g_metrics.peerHits++;
                    g_latency.push_back(role == ROLE_MEC ? 15.0 : 20.0);
                    currentTasks--;
                    return;
                }
            }
        }
        
        // Fallback to CDN
        if (cdnNode) {
            cdnNode->OnRequest(contentId, nullptr, false);
            g_metrics.upstreamHits++;
            g_latency.push_back(role == ROLE_MEC ? 25.0 : (role == ROLE_FOG ? 30.0 : 10.0));
        } else {
            g_metrics.upstreamHits++;
            g_latency.push_back(50.0);
        }
        
        // Cache the content for future requests
        double popularity = tracker ? tracker->getPopularity(contentId) : 1.0;
        cache.put(contentId, 1024 * 1024, popularity);
        
        currentTasks--;
    }
    
    Role getRole() const { return role; }
    const std::string& getLabel() const { return label; }
    PopLRU& getCache() { return cache; }
};

// User application that generates requests
class UserApp : public Application {
private:
    std::vector<std::string> catalog;
    std::function<void(const std::string&)> requestCallback;
    Ptr<ExponentialRandomVariable> intervalRv;
    Ptr<UniformRandomVariable> contentRv;
    
public:
    UserApp() : Application() {}
    
    void Setup(const std::vector<std::string>& cat, 
               std::function<void(const std::string&)> callback) {
        catalog = cat;
        requestCallback = callback;
        
        intervalRv = CreateObject<ExponentialRandomVariable>();
        intervalRv->SetAttribute("Mean", DoubleValue(20.0)); // 20ms mean interval
        
        contentRv = CreateObject<UniformRandomVariable>();
        contentRv->SetAttribute("Min", DoubleValue(0));
        contentRv->SetAttribute("Max", DoubleValue(catalog.size() - 1));
    }
    
    void StartApplication() override {
        ScheduleNextRequest();
    }
    
    void StopApplication() override {
        // Cleanup if needed
    }
    
private:
    void ScheduleNextRequest() {
        if (catalog.empty()) return;
        
        // Generate request
        int contentIndex = static_cast<int>(contentRv->GetValue());
        std::string contentId = catalog[contentIndex];
        
        if (requestCallback) {
            requestCallback(contentId);
        }
        
        // Schedule next request
        double nextInterval = intervalRv->GetValue();
        Simulator::Schedule(MilliSeconds(nextInterval), &UserApp::ScheduleNextRequest, this);
    }
};

// ------------------------------------------------------------------
// Scenario builder
// ------------------------------------------------------------------
struct TierDesc {
    uint32_t users = 0, nodes = 0, cacheMB = 0;
    SmartEdgeNode::Role role;
    bool p2p = false;
};

static std::vector<std::unique_ptr<SmartEdgeNode>> g_allNodes;

void CreateScenario(const TierDesc& mecD,
                    const TierDesc& fogD,
                    const TierDesc& cdnD,
                    const std::vector<std::string>& catalog,
                    uint32_t scenario) {
    static PopularityTracker tracker;
    static P2PManager p2pMec, p2pFog;

    NodeContainer cdn, mecNodes, fogNodes, mecUsers, fogUsers;
    InternetStackHelper stack;

    // CDN tier
    if (cdnD.nodes > 0) {
        cdn.Create(cdnD.nodes);
        stack.Install(cdn);
        for (uint32_t i = 0; i < cdnD.nodes; ++i) {
            auto cdnObj = std::make_unique<SmartEdgeNode>(SmartEdgeNode::ROLE_CDN,
                                  cdnD.cacheMB * 1024ULL * 1024ULL,
                                  &tracker, nullptr, cdn.Get(i), "CDN-" + std::to_string(i));
            cdnObj->InitializeCatalog(catalog);
            g_allNodes.push_back(std::move(cdnObj));
        }
    }

    // MEC tier
    if (mecD.nodes > 0) {
        mecNodes.Create(mecD.nodes);
        mecUsers.Create(mecD.users);
        stack.Install(mecNodes); 
        stack.Install(mecUsers);

        std::vector<SmartEdgeNode*> mecPtrs;
        for (uint32_t i = 0; i < mecD.nodes; ++i) {
            auto n = std::make_unique<SmartEdgeNode>(mecD.role,
                                      mecD.cacheMB * 1024ULL * 1024ULL,
                                      &tracker,
                                      mecD.p2p ? &p2pMec : nullptr,
                                      mecNodes.Get(i),
                                      "MEC-" + std::to_string(i));
            n->InitializeCatalog(catalog);
            mecPtrs.push_back(n.get());
            g_allNodes.push_back(std::move(n));
        }
        if (mecD.p2p) p2pMec.SetPeers(mecPtrs);

        for (uint32_t u = 0; u < mecUsers.GetN(); ++u) {
            Ptr<UserApp> app = CreateObject<UserApp>();
            SmartEdgeNode* node = mecPtrs[u % mecPtrs.size()];
            SmartEdgeNode* cdnNode = cdnD.nodes > 0 ? g_allNodes[0].get() : nullptr;
            app->Setup(catalog, [node, cdnNode](const std::string& id){
                node->OnRequest(id, cdnNode, true);
            });
            mecUsers.Get(u)->AddApplication(app);
            app->SetStartTime(Seconds(1.0 + 0.01 * u));
            app->SetStopTime(Seconds(299.9));
        }
    }

    // FOG tier
    if (fogD.nodes > 0) {
        fogNodes.Create(fogD.nodes);
        fogUsers.Create(fogD.users);
        stack.Install(fogNodes); 
        stack.Install(fogUsers);

        std::vector<SmartEdgeNode*> fogPtrs;
        for (uint32_t i = 0; i < fogD.nodes; ++i) {
            auto n = std::make_unique<SmartEdgeNode>(fogD.role,
                                      fogD.cacheMB * 1024ULL * 1024ULL,
                                      &tracker,
                                      fogD.p2p ? &p2pFog : nullptr,
                                      fogNodes.Get(i),
                                      "FOG-" + std::to_string(i));
            n->InitializeCatalog(catalog);
            fogPtrs.push_back(n.get());
            g_allNodes.push_back(std::move(n));
        }
        if (fogD.p2p) p2pFog.SetPeers(fogPtrs);

        for (uint32_t u = 0; u < fogUsers.GetN(); ++u) {
            Ptr<UserApp> app = CreateObject<UserApp>();
            SmartEdgeNode* node = fogPtrs[u % fogPtrs.size()];
            SmartEdgeNode* cdnNode = cdnD.nodes > 0 ? g_allNodes[0].get() : nullptr;
            app->Setup(catalog, [node, cdnNode](const std::string& id){
                node->OnRequest(id, cdnNode, true);
            });
            fogUsers.Get(u)->AddApplication(app);
            app->SetStartTime(Seconds(1.0 + 0.01 * u));
            app->SetStopTime(Seconds(299.9));
        }
    }
}

// ------------------------------------------------------------------
// CSV logging
// ------------------------------------------------------------------
void WriteCsv(const std::string& logDir, const std::string& tag, uint32_t scenario) {
    std::filesystem::create_directories(logDir);
    
    // Write summary
    std::ofstream summary(logDir + "/summary.csv");
    summary << "tag,scenario,req,served,local,peer,up,hL,hP,hU\n";
    
    uint64_t totalServed = g_metrics.localHits + g_metrics.peerHits + g_metrics.upstreamHits;
    double localRatio = totalServed > 0 ? (double)g_metrics.localHits / totalServed : 0.0;
    double peerRatio = totalServed > 0 ? (double)g_metrics.peerHits / totalServed : 0.0;
    double upstreamRatio = totalServed > 0 ? (double)g_metrics.upstreamHits / totalServed : 0.0;
    
    summary << tag << "," << scenario << "," 
            << g_metrics.totalRequests << "," << totalServed << ","
            << g_metrics.localHits << "," << g_metrics.peerHits << "," << g_metrics.upstreamHits << ","
            << localRatio << "," << peerRatio << "," << upstreamRatio << "\n";
    
    // Write node details
    std::ofstream nodes(logDir + "/nodes.csv");
    nodes << "tag,scenario,node,label,role,req,local,peer,up,cacheSz,cacheCap\n";
    
    for (size_t i = 0; i < g_allNodes.size(); ++i) {
        auto& node = g_allNodes[i];
        nodes << tag << "," << scenario << "," << i << ","
              << node->getLabel() << "," 
              << (node->getRole() == SmartEdgeNode::ROLE_MEC ? "MEC" : 
                  node->getRole() == SmartEdgeNode::ROLE_FOG ? "FOG" : "CDN") << ","
              << "0,0,0,0,"  // Placeholder for node-specific metrics
              << node->getCache().getSize() << "," << node->getCache().getCapacity() << "\n";
    }
}

// ------------------------------------------------------------------
// main
// ------------------------------------------------------------------
int main(int argc, char** argv) {
    RngSeedManager::SetSeed(1);
    RngSeedManager::SetRun(1);

    uint32_t scenario = 1;
    std::string tag = "default";
    double duration = 300.0;
    std::string logDir = "results";
    
    // Large-scale parameters
    uint32_t numUsers = 1000;
    uint32_t numMecNodes = 20;
    uint32_t numFogNodes = 10;
    uint32_t numCdnNodes = 1;
    uint32_t mecCacheMB = 256;
    uint32_t fogCacheMB = 128;
    uint32_t cdnCacheMB = 4000;

    CommandLine cmd(__FILE__);
    cmd.AddValue("scenario", "1:CDN-only 2:MEC+CDN 3:FOG+CDN 4:MEC+FOG+CDN+P2P", scenario);
    cmd.AddValue("runTag",   "Run tag for logging", tag);
    cmd.AddValue("duration", "Simulation duration in seconds", duration);
    cmd.AddValue("logDir",   "Directory to write CSV logs", logDir);
    cmd.AddValue("users", "Number of users", numUsers);
    cmd.AddValue("mecNodes", "Number of MEC nodes", numMecNodes);
    cmd.AddValue("fogNodes", "Number of FOG nodes", numFogNodes);
    cmd.AddValue("cdnNodes", "Number of CDN nodes", numCdnNodes);
    cmd.AddValue("mecCacheMB", "MEC cache size in MB", mecCacheMB);
    cmd.AddValue("fogCacheMB", "FOG cache size in MB", fogCacheMB);
    cmd.AddValue("cdnCacheMB", "CDN cache size in MB", cdnCacheMB);
    cmd.Parse(argc, argv);

    Time::SetResolution(Time::NS);

    const std::vector<std::string> catalog = {
        "video-4k-demo","video-1080p-news","game-arena","game-rally",
        "ar-guide","aug-tour","doc-whitepaper","doc-report",
        "video-8k-demo","video-4k-movie","game-fps","game-mmo",
        "ar-navigation","aug-education","doc-manual","doc-api"
    };
    NS_ASSERT(!catalog.empty());

    TierDesc mecD{0,0,0,SmartEdgeNode::ROLE_MEC,false};
    TierDesc fogD{0,0,0,SmartEdgeNode::ROLE_FOG,false};
    TierDesc cdnD{0,0,0,SmartEdgeNode::ROLE_CDN,false};

    switch (scenario) {
        case 1: // CDN-only
            cdnD = {numUsers, numCdnNodes, cdnCacheMB, SmartEdgeNode::ROLE_CDN, false}; 
            break;
        case 2: // MEC + CDN
            mecD = {numUsers, numMecNodes, mecCacheMB, SmartEdgeNode::ROLE_MEC, false}; 
            cdnD = {0, numCdnNodes, cdnCacheMB, SmartEdgeNode::ROLE_CDN, false};
            break;
        case 3: // FOG + CDN  
            fogD = {numUsers, numFogNodes, fogCacheMB, SmartEdgeNode::ROLE_FOG, false}; 
            cdnD = {0, numCdnNodes, cdnCacheMB, SmartEdgeNode::ROLE_CDN, false};
            break;
        case 4: // MEC + FOG + CDN + P2P
            mecD = {numUsers, numMecNodes, mecCacheMB, SmartEdgeNode::ROLE_MEC, true};
            fogD = {numUsers, numFogNodes, fogCacheMB, SmartEdgeNode::ROLE_FOG, true};
            cdnD = {0, numCdnNodes, cdnCacheMB, SmartEdgeNode::ROLE_CDN, false};
            break;
        default: NS_FATAL_ERROR("unknown scenario");
    }

    CreateScenario(mecD, fogD, cdnD, catalog, scenario);

    Simulator::Stop(Seconds(duration));
    Simulator::Run();
    Simulator::Destroy();

    WriteCsv(logDir, tag, scenario);

    g_allNodes.clear();
    return 0;
}