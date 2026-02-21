#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <chrono>
#include <atomic>
#include "storage/kv_store.hpp"

using namespace dkv;

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

int main() {
    std::cout << "\n=== Distributed KV Store Tests ===\n\n";

    test_basic_operations();
    test_concurrent_reads();
    test_concurrent_writes();
    test_mixed_workload();

    std::cout << "=== All tests passed ===\n\n";
    return 0;
}
