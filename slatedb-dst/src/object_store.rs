use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::RwLock;
use slatedb::clock::SystemClock;

/// ObjectStore wrapper that overrides metadata times using a provided SystemClock.
/// - Records timestamps for mutating operations (put, copy, rename, delete).
/// - Uses recorded timestamps for `last_modified` in ObjectMeta returned by `head` and `list`.
#[derive(Clone)]
pub struct ClockedObjectStore {
    inner: Arc<dyn ObjectStore>,
    clock: Arc<dyn SystemClock>,
    times: Arc<RwLock<HashMap<Path, DateTime<Utc>>>>,
}

impl ClockedObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, clock: Arc<dyn SystemClock>) -> Self {
        Self {
            inner,
            clock,
            times: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record a modification for the given path. If the path doesn't exist in the map,
    /// initialize both created and modified to now().
    fn record_modified(&self, path: &Path) -> DateTime<Utc> {
        let now = self.clock.now();
        let mut guard = self.times.write();
        guard
            .entry(path.clone())
            .and_modify(|t| *t = now)
            .or_insert_with(|| now);
        now
    }
}

impl fmt::Debug for ClockedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClockedObjectStore({})", self.inner)
    }
}

impl fmt::Display for ClockedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClockedObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for ClockedObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let res = self.inner.put_opts(location, payload, opts).await?;
        self.record_modified(location);
        Ok(res)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let res = self.inner.put_multipart_opts(location, opts).await;
        self.record_modified(location);
        res
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let times = self.times.clone();
        self.inner
            .list(prefix)
            .map(move |res| match res {
                Ok(meta) => {
                    let guard = times.read();
                    if let Some(t) = guard.get(&meta.location) {
                        return Ok(ObjectMeta {
                            last_modified: *t,
                            ..meta
                        });
                    }
                    Ok(meta)
                }
                Err(e) => Err(e),
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let times = self.times.clone();
        self.inner
            .list_with_offset(prefix, offset)
            .map(move |res| match res {
                Ok(meta) => {
                    let guard = times.read();
                    if let Some(t) = guard.get(&meta.location) {
                        return Ok(ObjectMeta {
                            last_modified: *t,
                            ..meta
                        });
                    }
                    Ok(meta)
                }
                Err(e) => Err(e),
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        // Map times for each object
        let guard = self.times.read();
        result.objects = result
            .objects
            .into_iter()
            .map(|meta| {
                if let Some(t) = guard.get(&meta.location) {
                    ObjectMeta {
                        last_modified: *t,
                        ..meta
                    }
                } else {
                    meta
                }
            })
            .collect();
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        use futures::TryStreamExt;
        use object_store::ObjectStoreExt;

        let inner = self.inner.clone();
        let times = self.times.clone();
        locations
            .and_then(move |location| {
                let inner = inner.clone();
                let times = times.clone();
                async move {
                    inner.delete(&location).await?;
                    times.write().remove(&location);
                    Ok(location)
                }
            })
            .boxed()
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        let res = self.inner.copy_opts(from, to, opts).await?;
        self.record_modified(to);
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};
    use slatedb::clock::MockSystemClock;

    fn p(s: &str) -> Path {
        Path::from(s)
    }

    #[tokio::test]
    async fn test_put_and_head_use_clock_time() {
        let clock = Arc::new(MockSystemClock::new());
        clock.set(1_234);
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        let path = p("foo");
        store
            .put(&path, PutPayload::from(b"data".as_slice()))
            .await
            .unwrap();

        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 1_234);
    }

    #[tokio::test]
    async fn test_list_overrides_times() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(1_000);
        store
            .put(&p("a"), PutPayload::from(b"a".as_slice()))
            .await
            .unwrap();

        clock.set(2_000);
        store
            .put(&p("b"), PutPayload::from(b"b".as_slice()))
            .await
            .unwrap();

        let items: Vec<_> = store
            .list(None)
            .try_collect()
            .await
            .expect("listing should succeed");

        let mut map = std::collections::HashMap::new();
        for m in items {
            map.insert(m.location.to_string(), m.last_modified.timestamp_millis());
        }
        assert_eq!(map.get("a"), Some(&1_000));
        assert_eq!(map.get("b"), Some(&2_000));
    }

    #[tokio::test]
    async fn test_delete_removes_entry() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(3_000);
        let path = p("todel");
        store
            .put(&path, PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        let meta = store.head(&path).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 3_000);

        store.delete(&path).await.unwrap();
        let head_res = store.head(&path).await;
        assert!(head_res.is_err(), "deleted object should not have head");
    }

    #[tokio::test]
    async fn test_rename_updates_target_and_removes_source() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(4_000);
        store
            .put(&p("src"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        let meta = store.head(&p("src")).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 4_000);

        clock.set(5_000);
        store.rename(&p("src"), &p("dst")).await.unwrap();

        // Source should be gone
        assert!(store.head(&p("src")).await.is_err());
        // Dest should have new timestamp
        let meta = store.head(&p("dst")).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 5_000);
    }

    #[tokio::test]
    async fn test_copy_updates_target_time() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(6_000);
        store
            .put(&p("a"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        let meta = store.head(&p("a")).await.unwrap();
        assert_eq!(meta.last_modified.timestamp_millis(), 6_000);

        clock.set(7_000);
        store.copy(&p("a"), &p("b")).await.unwrap();
        let meta_b = store.head(&p("b")).await.unwrap();
        assert_eq!(meta_b.last_modified.timestamp_millis(), 7_000);
    }

    #[tokio::test]
    async fn test_list_with_delimiter_overrides_times() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(8_000);
        store
            .put(&p("dir/a"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        clock.set(9_000);
        store
            .put(&p("dir/b"), PutPayload::from(b"y".as_slice()))
            .await
            .unwrap();

        let res = store
            .list_with_delimiter(Some(&p("dir/")))
            .await
            .expect("list_with_delimiter should succeed");
        assert_eq!(res.common_prefixes.len(), 0);
        let mut map = std::collections::HashMap::new();
        for m in res.objects {
            map.insert(m.location.to_string(), m.last_modified.timestamp_millis());
        }
        assert_eq!(map.get("dir/a"), Some(&8_000));
        assert_eq!(map.get("dir/b"), Some(&9_000));
    }

    #[tokio::test]
    async fn test_list_with_offset_overrides_times() {
        let clock = Arc::new(MockSystemClock::new());
        let inner = Arc::new(InMemory::new());
        let store = ClockedObjectStore::new(inner, clock.clone());

        clock.set(10_000);
        store
            .put(&p("a"), PutPayload::from(b"x".as_slice()))
            .await
            .unwrap();
        clock.set(11_000);
        store
            .put(&p("b"), PutPayload::from(b"y".as_slice()))
            .await
            .unwrap();

        let items: Vec<_> = store
            .list_with_offset(None, &p("a"))
            .try_collect()
            .await
            .expect("list_with_offset should succeed");
        // Should include at least "b" with its timestamp
        let mut map = std::collections::HashMap::new();
        for m in items {
            map.insert(m.location.to_string(), m.last_modified.timestamp_millis());
        }
        assert_eq!(map.get("b"), Some(&11_000));
    }
}
