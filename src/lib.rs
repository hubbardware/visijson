pub mod api;
pub mod auth;
pub mod error;
pub mod formulas;
pub mod meta;
pub mod model;
pub mod persistence;
pub mod stats;
pub mod store;
pub mod subscriptions;
pub mod typed;

// The 90% path, importable without knowing the module layout:
// `use visijson::{Store, TypedMount, Document, ...};`
pub use error::{Result, StoreError};
pub use meta::Stamp;
pub use model::{Node, PlainView, Pointer};
pub use persistence::Durability;
pub use store::{
    CommitCtx, CommitInfo, Committed, Condition, Op, Store, StoreConfig, WriteEffect, WriteGuard,
};
pub use typed::{to_node, Document, TypedMount};
