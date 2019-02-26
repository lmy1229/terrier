#pragma once
#include <algorithm>
#include <set>
#include <utility>
#include "common/spin_latch.h"
#include "transaction/transaction_defs.h"

namespace terrier::transaction {
/**
 * A TransactionThreadContext encapsulates information about the thread on which the transaction is started
 * (and presumably will finish). While this is not essential to our concurrency control algorithm, having
 * this information tagged with each transaction helps with various performance optimizations.
 */
class TransactionThreadContext {
 public:
  /**
   * Constructs a new TransactionThreadContext with the given worker_id
   * @param worker_id the worker_id of the thread
   */
  explicit TransactionThreadContext(worker_id_t worker_id) : worker_id_(worker_id) {}

  /**
   * @return worker id of the thread
   */
  worker_id_t GetWorkerId() const { return worker_id_; }

  /**
   * @return the smallest transaction id in the thread
   */
  timestamp_t GetOldestTransactionIdOrDefault(timestamp_t default_time) const {
    common::SpinLatch::ScopedSpinLatch guard(&thread_context_latch_);
    const auto &oldest_txn = std::min_element(curr_running_txn_ids_.cbegin(), curr_running_txn_ids_.cend());
    const timestamp_t result = (oldest_txn != curr_running_txn_ids_.end()) ? *oldest_txn : default_time;
    return result;
  }

  void startTransaction(timestamp_t txn_id) {
    common::SpinLatch::ScopedSpinLatch guard(&thread_context_latch_);
    curr_running_txn_ids_.emplace(txn_id);
  }
  void finishTransaction(timestamp_t txn_id) {
    common::SpinLatch::ScopedSpinLatch guard(&thread_context_latch_);
    const size_t ret UNUSED_ATTRIBUTE = curr_running_txn_ids_.erase(txn_id);
    TERRIER_ASSERT(ret == 1, "Committed transaction did not exist in thread transactions table");
  }
  bool is_running() const { return !curr_running_txn_ids_.empty(); }

  void unregister() { unregistered_ = true; }
  bool is_unregistered() const { return unregistered_; }

  void CompleteTransaction(TransactionContext *txn) {
    common::SpinLatch::ScopedSpinLatch guard(&thread_context_latch_);
    completed_txns_.push_front(txn);
  }

  void MergeCompletedTransactions(const TransactionQueue &queue) {
    common::SpinLatch::ScopedSpinLatch guard(&thread_context_latch_);
    queue.splice_after(queue.cbefore_begin(), std::move(completed_txns_));
  }

 private:
  // id of the worker thread on which the transaction start and finish.
  worker_id_t worker_id_;

  // set of id of the transaction the worker thread is currently working on.
  std::set<timestamp_t> curr_running_txn_ids_;

  // per-thread version of completed_txns_ in transaction_manager
  TransactionQueue completed_txns_;

  bool unregistered_;

  mutable common::SpinLatch thread_context_latch_;
};
}  // namespace terrier::transaction
