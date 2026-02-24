#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <chrono>
#include <atomic>
#include <filesystem>
#include "storage/kv_store.hpp"
#include "storage/persistent_kv_store.hpp"
#include "storage/lsm_tree.hpp"

using namespace dkv;

const std::string TEST_DATA_DIR = "./test_data";

void test_basic_operations() {
    std::cout << "[TEST] Basic Operations\n";
    KVStore store;

    assert(store.put("name", "Yash") == true);
    assert(store.put("name", "Yash Gulhane") == false);

    auto value = store.get("name");
    assert(value.has_value() && value.value() == "Yash Gulhane");
    assert(!store.get("unknown").has_value());

    assert(store.contains("name") == true);
    assert(store.contains("unknown") == false);

    store.put("city", "Delhi");
    store.put("company", "Samsung");
    assert(store.size() == 3);

    assert(store.del("city") == true);
    assert(store.del("city") == false);
    assert(store.size() == 2);

    std::cout << "[PASS] Basic Operations\n\n";
}

void test_concurrent_reads() {
    std::cout << "[TEST] Concurrent Reads\n";
    KVStore store;

    for (int i = 0; i < 1000; i++) {
        store.put("key" + std::to_string(i), "value" + std::to_string(i));
    }

    constexpr int NUM_READERS = 4;
    constexpr int READS_PER_THREAD = 10000;
    std::vector<std::thread> readers;

    auto start = std::chrono::high_resolution_clock::now();

    for (int t = 0; t < NUM_READERS; t++) {
        readers.emplace_back([&store, t]() {
            for (int i = 0; i < READS_PER_THREAD; i++) {
                int idx = (t * READS_PER_THREAD + i) % 1000;
                auto val = store.get("key" + std::to_string(idx));
                assert(val.has_value());
            }
        });
    }

    for (auto& t : readers) t.join();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start
    );

    std::cout << "  " << (NUM_READERS * READS_PER_THREAD) << " reads in "
              << duration.count() << "ms\n";
    std::cout << "[PASS] Concurrent Reads\n\n";
}

void test_concurrent_writes() {
    std::cout << "[TEST] Concurrent Writes\n";
    KVStore store;

    constexpr int NUM_WRITERS = 4;
    constexpr int WRITES_PER_THREAD = 1000;
    std::vector<std::thread> writers;

    auto start = std::chrono::high_resolution_clock::now();

    for (int t = 0; t < NUM_WRITERS; t++) {
        writers.emplace_back([&store, t]() {
            for (int i = 0; i < WRITES_PER_THREAD; i++) {
                store.put("t" + std::to_string(t) + "_k" + std::to_string(i), "v");
            }
        });
    }

    for (auto& t : writers) t.join();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start
    );

    assert(store.size() == NUM_WRITERS * WRITES_PER_THREAD);

    std::cout << "  " << (NUM_WRITERS * WRITES_PER_THREAD) << " writes in "
              << duration.count() << "ms\n";
    std::cout << "[PASS] Concurrent Writes\n\n";
}

void test_mixed_workload() {
    std::cout << "[TEST] Mixed Workload (80% read, 20% write)\n";
    KVStore store;

    for (int i = 0; i < 100; i++) {
        store.put("key" + std::to_string(i), "value" + std::to_string(i));
    }

    constexpr int NUM_THREADS = 8;
    constexpr int OPS_PER_THREAD = 5000;
    std::vector<std::thread> threads;
    std::atomic<int> reads{0}, writes{0};

    auto start = std::chrono::high_resolution_clock::now();

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back([&]() {
            for (int i = 0; i < OPS_PER_THREAD; i++) {
                if (i % 10 < 8) {
                    store.get("key" + std::to_string(i % 100));
                    reads++;
                } else {
                    store.put("key" + std::to_string(i % 100), "updated");
                    writes++;
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start
    );

    std::cout << "  Reads: " << reads << ", Writes: " << writes
              << " in " << duration.count() << "ms\n";
    std::cout << "[PASS] Mixed Workload\n\n";
}

void cleanup_test_dir() {
    std::filesystem::remove_all(TEST_DATA_DIR);
}

void test_persistence_basic() {
    std::cout << "[TEST] Persistence Basic\n";
    cleanup_test_dir();

    {
        PersistentKVStore store(TEST_DATA_DIR);
        store.put("name", "Yash");
        store.put("city", "Delhi");
        store.put("lang", "C++");
        store.del("city");
    }

    {
        PersistentKVStore store(TEST_DATA_DIR);
        assert(store.size() == 2);
        assert(store.get("name").value() == "Yash");
        assert(store.get("lang").value() == "C++");
        assert(!store.get("city").has_value());
    }

    cleanup_test_dir();
    std::cout << "[PASS] Persistence Basic\n\n";
}

void test_persistence_recovery() {
    std::cout << "[TEST] Persistence Recovery (simulated crash)\n";
    cleanup_test_dir();

    {
        PersistentKVStore store(TEST_DATA_DIR);
        for (int i = 0; i < 100; i++) {
            store.put("key" + std::to_string(i), "value" + std::to_string(i));
        }
        for (int i = 0; i < 50; i += 2) {
            store.del("key" + std::to_string(i));
        }
    }

    {
        PersistentKVStore store(TEST_DATA_DIR);
        assert(store.size() == 75);
        assert(!store.get("key0").has_value());
        assert(store.get("key1").value() == "value1");
        assert(!store.get("key48").has_value());
        assert(store.get("key99").value() == "value99");
    }

    cleanup_test_dir();
    std::cout << "[PASS] Persistence Recovery\n\n";
}

void test_persistence_checkpoint() {
    std::cout << "[TEST] Persistence Checkpoint\n";
    cleanup_test_dir();

    {
        PersistentKVStore store(TEST_DATA_DIR);
        for (int i = 0; i < 1000; i++) {
            store.put("key" + std::to_string(i), "value" + std::to_string(i));
        }
        store.clear();
        store.put("after", "checkpoint");
    }

    {
        PersistentKVStore store(TEST_DATA_DIR);
        assert(store.size() == 1);
        assert(store.get("after").value() == "checkpoint");
    }

    cleanup_test_dir();
    std::cout << "[PASS] Persistence Checkpoint\n\n";
}

void test_persistence_performance() {
    std::cout << "[TEST] Persistence Performance\n";
    cleanup_test_dir();

    constexpr int NUM_OPS = 10000;

    auto start = std::chrono::high_resolution_clock::now();
    {
        PersistentKVStore store(TEST_DATA_DIR);
        for (int i = 0; i < NUM_OPS; i++) {
            store.put("key" + std::to_string(i), "value" + std::to_string(i));
        }
    }
    auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start
    );

    start = std::chrono::high_resolution_clock::now();
    {
        PersistentKVStore store(TEST_DATA_DIR);
        assert(store.size() == NUM_OPS);
    }
    auto recover_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start
    );

    std::cout << "  " << NUM_OPS << " writes in " << write_duration.count() << "ms\n";
    std::cout << "  Recovery in " << recover_duration.count() << "ms\n";

    cleanup_test_dir();
    std::cout << "[PASS] Persistence Performance\n\n";
}

const std::string LSM_TEST_DIR = "./lsm_test_data";

void cleanup_lsm_dir() {
    std::filesystem::remove_all(LSM_TEST_DIR);
}

void test_lsm_basic() {
    std::cout << "[TEST] LSM Basic Operations\n";
    cleanup_lsm_dir();

    {
        LSMTree lsm(LSM_TEST_DIR);
        
        lsm.put("name", "Yash");
        lsm.put("city", "Delhi");
        lsm.put("lang", "C++");
        
        assert(lsm.get("name").value() == "Yash");
        assert(lsm.get("city").value() == "Delhi");
        assert(lsm.get("lang").value() == "C++");
        
        lsm.del("city");
        assert(!lsm.get("city").has_value());
        
        lsm.put("name", "Yash Gulhane");
        assert(lsm.get("name").value() == "Yash Gulhane");
    }

    cleanup_lsm_dir();
    std::cout << "[PASS] LSM Basic Operations\n\n";
}

void test_lsm_flush() {
    std::cout << "[TEST] LSM Flush to SSTable\n";
    cleanup_lsm_dir();

    {
        LSMConfig config;
        config.memtable_size_limit = 1024;
        LSMTree lsm(LSM_TEST_DIR, config);
        
        for (int i = 0; i < 100; i++) {
            lsm.put("key" + std::to_string(i), std::string(50, 'a' + (i % 26)));
        }
        
        assert(lsm.sstableCount() > 0);
        std::cout << "  Created " << lsm.sstableCount() << " SSTables\n";
        
        for (int i = 0; i < 100; i++) {
            auto val = lsm.get("key" + std::to_string(i));
            assert(val.has_value());
            assert(val.value() == std::string(50, 'a' + (i % 26)));
        }
    }

    cleanup_lsm_dir();
    std::cout << "[PASS] LSM Flush to SSTable\n\n";
}

void test_lsm_recovery() {
    std::cout << "[TEST] LSM Recovery\n";
    cleanup_lsm_dir();

    {
        LSMConfig config;
        config.memtable_size_limit = 1024;
        LSMTree lsm(LSM_TEST_DIR, config);
        
        for (int i = 0; i < 200; i++) {
            lsm.put("key" + std::to_string(i), "value" + std::to_string(i));
        }
        for (int i = 0; i < 100; i += 2) {
            lsm.del("key" + std::to_string(i));
        }
    }

    {
        LSMConfig config;
        config.memtable_size_limit = 1024;
        LSMTree lsm(LSM_TEST_DIR, config);
        
        assert(!lsm.get("key0").has_value());
        assert(lsm.get("key1").value() == "value1");
        assert(!lsm.get("key98").has_value());
        assert(lsm.get("key99").value() == "value99");
        assert(lsm.get("key199").value() == "value199");
    }

    cleanup_lsm_dir();
    std::cout << "[PASS] LSM Recovery\n\n";
}

void test_lsm_large_dataset() {
    std::cout << "[TEST] LSM Large Dataset\n";
    cleanup_lsm_dir();

    constexpr int NUM_ENTRIES = 10000;
    
    auto start = std::chrono::high_resolution_clock::now();
    {
        LSMConfig config;
        config.memtable_size_limit = 64 * 1024;
        LSMTree lsm(LSM_TEST_DIR, config);
        
        for (int i = 0; i < NUM_ENTRIES; i++) {
            lsm.put("key" + std::to_string(i), "value" + std::to_string(i));
        }
    }
    auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start
    );
    
    start = std::chrono::high_resolution_clock::now();
    {
        LSMConfig config;
        config.memtable_size_limit = 64 * 1024;
        LSMTree lsm(LSM_TEST_DIR, config);
        
        int found = 0;
        for (int i = 0; i < NUM_ENTRIES; i++) {
            if (lsm.get("key" + std::to_string(i)).has_value()) {
                found++;
            }
        }
        assert(found == NUM_ENTRIES);
        
        std::cout << "  SSTables: " << lsm.sstableCount() << "\n";
    }
    auto read_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::high_resolution_clock::now() - start
    );
    
    std::cout << "  " << NUM_ENTRIES << " writes in " << write_duration.count() << "ms\n";
    std::cout << "  " << NUM_ENTRIES << " reads in " << read_duration.count() << "ms\n";

    cleanup_lsm_dir();
    std::cout << "[PASS] LSM Large Dataset\n\n";
}

int main() {
    std::cout << "\n=== Distributed KV Store Tests ===\n\n";

    test_basic_operations();
    test_concurrent_reads();
    test_concurrent_writes();
    test_mixed_workload();

   
    test_persistence_basic();
    test_persistence_recovery();
    test_persistence_checkpoint();
    test_persistence_performance();

    test_lsm_basic();
    test_lsm_flush();
    test_lsm_recovery();
    test_lsm_large_dataset();

    std::cout << "=== All tests passed ===\n\n";
    return 0;
}
