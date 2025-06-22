use chrono::{DateTime, Utc};
use futures::{stream, Stream};
use log::info;
use reqwest_middleware::ClientWithMiddleware;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChronError {
    #[error("Error building Chron request: {0}")]
    RequestBuild(reqwest::Error),

    #[error("Error searching cache for games page: {0}")]
    CacheGetError(sled::Error),

    #[error("Error executing Chron request: {0}")]
    RequestExecute(reqwest::Error),
    
    #[error("Error deserializing Chron response: {0}")]
    RequestDeserialize(reqwest::Error),

    #[error("Error encoding Chron response for cache: {0}")]
    CacheSerialize(rmp_serde::encode::Error),

    #[error("Error inserting games page into cache: {0}")]
    CachePutError(sled::Error),

    #[error("Error removing invalid games page from cache: {0}")]
    CacheRemoveError(sled::Error),

    #[error("Error flushing cache to disk: {0}")]
    CacheFlushError(sled::Error),
}

// This is exactly like ChronEntities except it contains its own page 
// token instead of the next page's token. The first page's token is
// None
#[derive(Debug, Serialize, Deserialize)]
pub struct ChronPage<EntityT> {
    pub items: Vec<ChronEntity<EntityT >>,
    pub page_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChronEntities<EntityT> {
    pub items: Vec<ChronEntity<EntityT>>,
    pub next_page: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChronEntity<EntityT> {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub data: EntityT,
}

pub struct Chron {
    cache: sled::Db,
    client: reqwest::Client,
    fetch_games_list_chunks_string: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum VersionedCacheEntry<T> {
    V0(T),
}

impl Chron {
    pub fn new<P: AsRef<std::path::Path>>(
        cache_path: P,
        fetch_games_list_chunks: usize,
    ) -> Result<Self, sled::Error> {
        let cache_path = cache_path.as_ref();
        let cache = sled::open(cache_path)?;
        if cache.was_recovered() {
            match cache.size_on_disk() {
                Ok(size) => { info!("Opened existing {size}-byte cache at {cache_path:?}") }
                Err(err) => { info!("Opened existing cache at {cache_path:?}. Error retrieving size: {err}") }
            }
        } else {
            info!("Created new cache at {cache_path:?}");
        }
        
        Ok(Self {
            cache,
            client: reqwest::Client::new(),
            fetch_games_list_chunks_string: fetch_games_list_chunks.to_string(),
        })
    }

    fn entities_request(&self, kind: &str, count: &str, page: Option<&str>) -> reqwest::RequestBuilder {
        let request = self.client
            .get("https://freecashe.ws/api/chron/v0/entities")
            .query(&[("kind", kind), ("count", count), ("order", "asc")]);

        if let Some(page_token) = page {
            request.query(&[("page", page_token)])
        } else {
            request
        }
    }

    fn get_cached<T: for<'d> Deserialize<'d>>(&self, key: &str) -> Result<Option<T>, ChronError> {
        let Some(cache_entry) = self.cache.get(key).map_err(ChronError::CacheGetError)? else {
            return Ok(None)
        };

        let versions = match rmp_serde::from_slice(&cache_entry) {
            Ok(versions) => versions,
            Err(err) => {
                info!("Cache entry could not be decoded: {:?}. Removing it from the cache.", err);
                self.cache.remove(key).map_err(ChronError::CacheRemoveError)?;
                return Ok(None);
            }
        };

        match versions {
            VersionedCacheEntry::V0(data) => Ok(Some(data)),
        }
    }

    // Must take owned `page` for lifetime reasons
    pub async fn games_page(&self, page: Option<String>) -> Result<ChronEntities<mmolb_parsing::Game>, ChronError> {
        let request = self.entities_request("game", &self.fetch_games_list_chunks_string, page.as_deref()).build()
            .map_err(ChronError::RequestBuild)?;
        let url = request.url().to_string();
        let result = if let Some(cache_entry) = self.get_cached(&url)? {
            info!("Returning page {page:?} from cache");
            Ok(cache_entry)
        } else {
            // Cache miss -- request from chron
            info!("Requesting page {page:?} from chron");
            let response = self.client.execute(request).await
                .map_err(ChronError::RequestExecute)?;
            let entities: ChronEntities<mmolb_parsing::Game> = response.json()
                .await
                .map_err(ChronError::RequestDeserialize)?;

            let cache_entry = VersionedCacheEntry::V0(entities);

            // Save to cache
            let entities_bin = rmp_serde::to_vec(&cache_entry)
                .map_err(ChronError::CacheSerialize)?;
            self.cache.insert(url.as_str(), entities_bin.as_slice()).map_err(ChronError::CachePutError)?;

            // Immediately fetch again from cache to verify everything is working
            let entities = self.get_cached(&url.as_str())
                .expect("Error getting cache entry immediately after it was saved")
                .expect("Cache entry was not found immediately after it was saved");
            
            Ok(entities)
        };
        
        // Fetches are already so slow that cache flushing should be a drop in the bucket. Non-fetch
        // requests shouldn't dirty the cache at all and so this should be near-instant.
        self.cache.flush().map_err(ChronError::CacheFlushError)?;
        
        result
    }
}
