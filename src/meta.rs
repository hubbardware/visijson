//! Per-path modification metadata: which commit last touched a subtree,
//! and when — the basis for HTTP `ETag` / `Last-Modified` validators.
//!
//! A read of path `P` is invalidated by a commit that changed path `C`
//! whenever the two **overlap** (either is a prefix of the other) — the
//! same relevance rule subscriptions use. [`ModIndex`] answers that query
//! without scanning history by storing, per recorded path, two stamps:
//! `own` (this exact path was reported changed) and `sub` (this path or
//! anything below it changed). Then
//!
//! ```text
//! last_modified(P) = max(sub[P], own[A] for every strict ancestor A of P)
//! ```
//!
//! — one lookup plus one per depth level, no descendant scan.
//!
//! The index is persistent (`OrdMap`): a new version shares all untouched
//! entries with the previous one, so carrying it in every `Committed` costs
//! only the paths a commit actually wrote. Snapshots carry a [`dump`]
//! (`ModIndex::dump`) of it, so recovery restores exact per-path validators
//! (client ETags stay valid across a restart) and WAL replay re-sharpens
//! from there; a snapshot without one (pre-dump format) falls back to the
//! floor.
//!
//! ## Floor: safe answers for what the index doesn't know
//!
//! Reporting a *newer* stamp than the truth only costs a needless refetch;
//! reporting an *older* one would hand a client a stale 304. So everything
//! the index cannot answer exactly falls back to a floor stamp, which is
//! always an over-estimate: the store's state when the index was last
//! reset (open/recovery, or a collapse once [`MAX_ENTRIES`] is exceeded).

use imbl::OrdMap;

use crate::model::Pointer;

/// A point in the commit stream: version plus wall-clock time. Versions
/// are the authority for ordering; the timestamp is only for HTTP dates.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stamp {
    pub version: u64,
    pub time_ms: u64,
}

impl Stamp {
    pub fn new(version: u64, time_ms: u64) -> Self {
        Stamp { version, time_ms }
    }

    /// The later of two stamps, by version.
    pub fn later(self, other: Stamp) -> Stamp {
        if other.version > self.version {
            other
        } else {
            self
        }
    }
}

/// Entries past this count collapse the index to its floor — bounded
/// memory in exchange for coarser (still safe) validators. Sized so a
/// document with a few hundred thousand distinct written paths stays
/// exact; pathological write fan-out degrades to whole-store validators.
pub const MAX_ENTRIES: usize = 200_000;

#[derive(Debug, Clone, Default)]
struct Entry {
    own: Stamp,
    sub: Stamp,
}

#[derive(Debug, Clone)]
pub struct ModIndex {
    entries: OrdMap<Pointer, Entry>,
    floor: Stamp,
}

/// `ModIndex` in snapshot-file form: `(path, own_version, own_ms,
/// sub_version, sub_ms)` rows plus the floor.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModIndexDump {
    floor: (u64, u64),
    entries: Vec<(String, u64, u64, u64, u64)>,
}

impl ModIndex {
    /// An index that knows nothing yet: every path reports `floor`.
    pub fn new(floor: Stamp) -> Self {
        ModIndex {
            entries: OrdMap::new(),
            floor,
        }
    }

    pub fn floor(&self) -> Stamp {
        self.floor
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Record that `changed` paths were written by the commit at `stamp`,
    /// returning the updated index (the receiver is left untouched, so old
    /// `Committed`s keep answering as of their own version).
    pub fn record<'a>(
        &self,
        stamp: Stamp,
        changed: impl IntoIterator<Item = &'a Pointer>,
    ) -> ModIndex {
        self.record_capped(stamp, changed, MAX_ENTRIES)
    }

    fn record_capped<'a>(
        &self,
        stamp: Stamp,
        changed: impl IntoIterator<Item = &'a Pointer>,
        cap: usize,
    ) -> ModIndex {
        let mut entries = self.entries.clone();
        for c in changed {
            // `own` for the changed path itself.
            let e = entries.entry(c.clone()).or_default();
            e.own = e.own.later(stamp);
            e.sub = e.sub.later(stamp);
            // `sub` for every ancestor, root included.
            let mut prefix = Pointer::root();
            for tok in c.tokens() {
                let e = entries.entry(prefix.clone()).or_default();
                e.sub = e.sub.later(stamp);
                prefix = prefix.push(tok.clone());
            }
        }
        if entries.len() > cap {
            tracing::info!(
                entries = entries.len(),
                version = stamp.version,
                "modification index exceeded cap; collapsing to whole-store validators"
            );
            return ModIndex::new(stamp);
        }
        ModIndex {
            entries,
            floor: self.floor,
        }
    }

    /// Compact serialized form for snapshots, so a restart keeps exact
    /// per-path validators instead of resetting every path to a floor.
    /// Bounded by [`MAX_ENTRIES`].
    pub fn dump(&self) -> ModIndexDump {
        ModIndexDump {
            floor: (self.floor.version, self.floor.time_ms),
            entries: self
                .entries
                .iter()
                .map(|(p, e)| {
                    (
                        p.to_string(),
                        e.own.version,
                        e.own.time_ms,
                        e.sub.version,
                        e.sub.time_ms,
                    )
                })
                .collect(),
        }
    }

    /// Rebuild from a snapshot dump. An unparsable path is skipped — the
    /// floor keeps its answers safe (over-estimates only).
    pub fn from_dump(d: &ModIndexDump) -> ModIndex {
        let mut entries = OrdMap::new();
        for (path, own_v, own_t, sub_v, sub_t) in &d.entries {
            let Ok(p) = Pointer::parse(path) else { continue };
            entries.insert(
                p,
                Entry {
                    own: Stamp::new(*own_v, *own_t),
                    sub: Stamp::new(*sub_v, *sub_t),
                },
            );
        }
        ModIndex {
            entries,
            floor: Stamp::new(d.floor.0, d.floor.1),
        }
    }

    /// When was the subtree at `p` last modified? Never older than the
    /// truth: unknown paths fall back to the floor.
    pub fn last_modified(&self, p: &Pointer) -> Stamp {
        let mut best = self.floor;
        if let Some(e) = self.entries.get(p) {
            best = best.later(e.sub);
        }
        // Strict ancestors: a rewrite above `p` modified `p` as well.
        let mut prefix = Pointer::root();
        for tok in p.tokens() {
            if let Some(e) = self.entries.get(&prefix) {
                best = best.later(e.own);
            }
            prefix = prefix.push(tok.clone());
        }
        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Pointer {
        Pointer::parse(s).unwrap()
    }

    #[test]
    fn tracks_ancestors_and_descendants_but_not_siblings() {
        let idx = ModIndex::new(Stamp::new(1, 100));
        let idx = idx.record(Stamp::new(2, 200), [&p("/a/b/c")]);

        // The changed path, its ancestors, and its descendants all moved.
        assert_eq!(idx.last_modified(&p("/a/b/c")).version, 2);
        assert_eq!(idx.last_modified(&p("/a/b")).version, 2);
        assert_eq!(idx.last_modified(&p("/a")).version, 2);
        assert_eq!(idx.last_modified(&Pointer::root()).version, 2);
        assert_eq!(idx.last_modified(&p("/a/b/c/deeper")).version, 2);

        // Siblings did not.
        assert_eq!(idx.last_modified(&p("/a/b/other")).version, 1);
        assert_eq!(idx.last_modified(&p("/z")).version, 1);
    }

    #[test]
    fn keeps_the_latest_stamp_per_path() {
        let idx = ModIndex::new(Stamp::new(1, 100))
            .record(Stamp::new(2, 200), [&p("/x")])
            .record(Stamp::new(3, 300), [&p("/y")]);
        assert_eq!(idx.last_modified(&p("/x")), Stamp::new(2, 200));
        assert_eq!(idx.last_modified(&p("/y")), Stamp::new(3, 300));
        assert_eq!(idx.last_modified(&Pointer::root()), Stamp::new(3, 300));
    }

    #[test]
    fn old_versions_keep_answering_as_of_themselves() {
        let v2 = ModIndex::new(Stamp::new(1, 100)).record(Stamp::new(2, 200), [&p("/x")]);
        let v3 = v2.record(Stamp::new(3, 300), [&p("/x")]);
        assert_eq!(v2.last_modified(&p("/x")).version, 2);
        assert_eq!(v3.last_modified(&p("/x")).version, 3);
    }

    #[test]
    fn floor_answers_paths_the_index_never_saw() {
        let idx = ModIndex::new(Stamp::new(7, 700));
        assert_eq!(idx.last_modified(&p("/never/written")), Stamp::new(7, 700));
    }

    #[test]
    fn dump_roundtrip_preserves_every_answer() {
        let idx = ModIndex::new(Stamp::new(1, 100))
            .record(Stamp::new(2, 200), [&p("/a/b/c")])
            .record(Stamp::new(3, 300), [&p("/x")]);
        let restored = ModIndex::from_dump(&idx.dump());
        for probe in ["/a/b/c", "/a/b", "/a", "/a/b/c/deeper", "/a/b/other", "/x", "/z", ""] {
            assert_eq!(
                restored.last_modified(&p(probe)),
                idx.last_modified(&p(probe)),
                "at {probe}"
            );
        }
    }

    #[test]
    fn collapse_keeps_answers_safe_never_stale() {
        let idx = ModIndex::new(Stamp::new(1, 100)).record(Stamp::new(2, 200), [&p("/kept")]);
        // A tiny cap forces the collapse; the whole index becomes a floor
        // at the collapsing commit's stamp.
        let collapsed = idx.record_capped(Stamp::new(3, 300), [&p("/a/b"), &p("/c/d")], 4);
        assert!(collapsed.is_empty());
        assert_eq!(collapsed.floor(), Stamp::new(3, 300));
        // Over-estimate invariant: every answer is >= the exact one — a
        // needless refetch, never a stale 304.
        for probe in ["/kept", "/a/b", "/c/d", "/never"] {
            let exact = idx
                .record(Stamp::new(3, 300), [&p("/a/b"), &p("/c/d")])
                .last_modified(&p(probe));
            assert!(
                collapsed.last_modified(&p(probe)).version >= exact.version,
                "stale answer at {probe}"
            );
        }
    }
}
