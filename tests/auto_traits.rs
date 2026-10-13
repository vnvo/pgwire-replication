//! The public types keep the auto traits callers may rely on.

use pgwire_replication::{ReplicationClient, ReplicationConfig};
use std::panic::{RefUnwindSafe, UnwindSafe};

fn assert_auto_traits<T: Send + Sync + Unpin + UnwindSafe + RefUnwindSafe>() {}

#[test]
fn public_types_keep_their_auto_traits() {
    assert_auto_traits::<ReplicationConfig>();
    assert_auto_traits::<ReplicationClient>();
}
