pub mod api;
pub mod error;
pub mod formulas;
pub mod meta;
pub mod model;
pub mod persistence;
pub mod store;
pub mod subscriptions;
pub mod typed;

// The 90% path, importable without knowing the module layout:
// `use visijson::{Store, TypedMount, Document, ...};`
pub use error::{Result, StoreError};
pub use meta::Stamp;
pub use model::{Node, PlainView, Pointer};
pub use store::{CommitInfo, Committed, Condition, Op, Store, StoreConfig};
pub use typed::{to_node, Document, TypedMount};
