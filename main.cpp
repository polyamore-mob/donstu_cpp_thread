#include <iostream>
#include <vector>
#include <thread>
#include <sstream>
#include <atomic>
#include <future>
#include <mutex>
#include <condition_variable>

#include "threadfuncs.h"

int main() {
  about();

  // Open log file
  Logger logger("output.log");

  logger.writeLine(
      "main: pid = " + std::to_string(getThreadID()) +
      ", opened file: 'output.log'"
  );

  // args for threads
  std::vector<ThreadArgs> args(COUNT_THREADS);

  for (int i = 0; i < COUNT_THREADS; ++i) {
    std::ostringstream oss;
    oss << "T" << i;
    args[i].id = i;
    args[i].tag = oss.str();
  }

  // thread are starting
  std::vector<std::thread> threads;
  threads.reserve(COUNT_THREADS);

  for (int i = 0; i < COUNT_THREADS; ++i) {
    threads.emplace_back(funcThread, std::cref(args[i]), std::ref(logger));
  }

  // compare std::thread::id
  std::thread::id id1 = threads[0].get_id();
  std::thread::id id2 = threads[1].get_id();

  bool same = (id1 == id2);

  logger.writeLine(
      std::string("thread ids equal: ") + (same ? "true" : "false")
  );

  // wait for stop all threads
  for (auto& t : threads) {
    if (t.joinable()) {
      t.join();
    }
  }

  // ----------------------------------------
  // Task 14/16: promise / future
  // ----------------------------------------

  std::promise<std::string> prom;
  std::future<std::string> fut = prom.get_future();

  std::thread resultThread(
      [prom = std::move(prom)]() mutable {
        int iterations = 0;

        for (int i = 0; i < COUNT_ITERATIONS; ++i) {
          ++iterations;
        }

        prom.set_value(std::to_string(iterations));
      }
  );

  std::string result = fut.get();

  resultThread.join();

  logger.writeLine("Результат потока: " + result);
  std::cout << "Результат потока: " << result << "\n";

  // ----------------------------------------
  // Task 20: ordinary int counter
  // ----------------------------------------

  int counter = 0;

  std::vector<std::thread> counterThreads;

  for (int i = 0; i < COUNT_THREADS; ++i) {
    counterThreads.emplace_back([&counter]() {
      for (int i = 0; i < 100000; ++i) {
        ++counter;
      }
    });
  }

  for (auto& t : counterThreads) {
    t.join();
  }

  std::cout << "ordinary int counter = " << counter << "\n";

  // ----------------------------------------
  // Task 20: atomic counter
  // ----------------------------------------

  std::atomic<int> atomicCounter{0};

  counterThreads.clear();

  for (int i = 0; i < COUNT_THREADS; ++i) {
    counterThreads.emplace_back([&atomicCounter]() {
      for (int i = 0; i < 100000; ++i) {
        ++atomicCounter;
      }
    });
  }

  for (auto& t : counterThreads) {
    t.join();
  }

  std::cout << "atomic counter = " << atomicCounter << "\n";

  // ----------------------------------------
  // Task 21: Producer-Consumer
  // ----------------------------------------

  std::mutex m;
  std::condition_variable cv;

  int buffer = 0;
  bool ready = false;
  bool done = false;

  std::thread producer([&]() {
    for (int value = 1; value <= 10; ++value) {
      {
        std::unique_lock<std::mutex> lock(m);

        cv.wait(lock, [&]() {
          return !ready;
        });

        buffer = value;
        ready = true;
      }

      cv.notify_one();
    }

    {
      std::lock_guard<std::mutex> lock(m);
      done = true;
    }

    cv.notify_one();
  });

  std::thread consumer([&]() {
    for (;;) {
      int value = 0;

      {
        std::unique_lock<std::mutex> lock(m);

        cv.wait(lock, [&]() {
          return ready || done;
        });

        if (!ready && done) {
          break;
        }

        value = buffer;
        ready = false;
      }

      logger.writeLine(
          "consumer: received " + std::to_string(value)
      );

      cv.notify_one();
    }
  });

  producer.join();
  consumer.join();

  // close file automatically
  logger.writeLine("main: all threads finished, file closed");

  return 0;
}
