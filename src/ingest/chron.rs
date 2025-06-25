use chrono::{DateTime, Utc};
use humansize::{format_size, DECIMAL};
use log::{info, warn};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChronError {
    #[error("Error building Chron request: {0}")]
    RequestBuildError(reqwest::Error),

    #[error("Error searching cache for games page: {0}")]
    CacheGetError(sled::Error),

    #[error("Error executing Chron request: {0}")]
    RequestExecuteError(reqwest::Error),
    
    #[error("Error deserializing Chron response: {0}")]
    RequestDeserializeError(reqwest::Error),

    #[error("Error encoding Chron response for cache: {0}")]
    CacheSerializeError(rmp_serde::encode::Error),

    #[error("Error inserting games page into cache: {0}")]
    CachePutError(sled::Error),

    #[error("Error removing invalid games page from cache: {0}")]
    CacheRemoveError(sled::Error),

    #[error("Error flushing cache to disk: {0}")]
    CacheFlushError(sled::Error),
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
    cache: Option<sled::Db>,
    client: reqwest::Client,
    page_size: usize,
    page_size_string: String,
}

#[derive(Debug, Serialize, Deserialize)]
enum VersionedCacheEntry<T> {
    V0(T),
}

pub trait GameExt {

    /// Returns true for any game which will never be updated. This includes all
    /// finished games and a set of games from s0d60 that the sim lost track of
    /// and will never be finished.
    fn is_terminal(&self) -> bool;

    /// True for any game in the "Completed" state
    fn is_completed(&self) -> bool;
}

impl GameExt for mmolb_parsing::Game {
    fn is_terminal(&self) -> bool {
        // There are some games from season 0 that aren't completed and never
        // will be.
        self.season == 0 || self.is_completed()
    }

    fn is_completed(&self) -> bool {
        self.state == "Complete"
    }
}

impl Chron {
    pub fn new<P: AsRef<std::path::Path>>(
        use_cache: bool,
        cache_path: P,
        page_size: usize,
    ) -> Result<Self, sled::Error> {
        let cache = if use_cache {
            info!("Opening cache db. This can be very slow, which is a known issue.");
            let cache_path = cache_path.as_ref();
            let cache = sled::open(cache_path)?;
            if cache.was_recovered() {
                match cache.size_on_disk() {
                    Ok(size) => {
                        let size_str = format_size(size, DECIMAL);
                        info!("Opened existing {size_str} cache at {cache_path:?}");
                    }
                    Err(err) => { info!("Opened existing cache at {cache_path:?}. Error retrieving size: {err}") }
                }
            } else {
                info!("Created new cache at {cache_path:?}");
            }

            Some(cache)
        } else {
            None
        };
        
        Ok(Self {
            cache,
            client: reqwest::Client::new(),
            page_size,
            page_size_string: page_size.to_string(),
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
        let Some(cache) = &self.cache else {
            return Ok(None);
        };
        
        let Some(cache_entry) = cache.get(key).map_err(ChronError::CacheGetError)? else {
            return Ok(None)
        };

        let versions = match rmp_serde::from_slice(&cache_entry) {
            Ok(versions) => versions,
            Err(err) => {
                warn!("Cache entry could not be decoded: {:?}. Removing it from the cache.", err);
                cache.remove(key).map_err(ChronError::CacheRemoveError)?;
                return Ok(None);
            }
        };

        match versions {
            VersionedCacheEntry::V0(data) => Ok(Some(data)),
        }
    }

    pub async fn games_page(&self, page: Option<&str>) -> Result<ChronEntities<mmolb_parsing::Game>, ChronError> {
        let request = self.entities_request("game", &self.page_size_string, page.as_deref()).build()
            .map_err(ChronError::RequestBuildError)?;
        let url = request.url().to_string();
        let result = if let Some(cache_entry) = self.get_cached(&url)? {
            info!("Returning page {page:?} from cache");
            Ok(cache_entry)
        } else {
            // Cache miss -- request from chron
            info!("Requesting page {page:?} from chron");
            let response = self.client.execute(request).await
                .map_err(ChronError::RequestExecuteError)?;
            let entities: ChronEntities<mmolb_parsing::Game> = response.json()
                .await
                .map_err(ChronError::RequestDeserializeError)?;
            
            let Some(cache) = &self.cache else {
                return Ok(entities)
            };

            if entities.next_page.is_none() {
                info!("Not caching page {page:?} because it's the last page");
                return Ok(entities);
            }

            if entities.items.len() != self.page_size {
                info!("Not caching page {page:?} because it's the last page");
                return Ok(entities);
            }

            let has_incomplete_game = entities.items.iter()
                .any(|item| !item.data.is_terminal());
            if has_incomplete_game {
                info!("Not caching page {page:?} because it contains at least one non-terminal game");
                return Ok(entities);
            }

            // Otherwise, save to cache
            let cache_entry = VersionedCacheEntry::V0(entities);

            // Save to cache
            let entities_bin = rmp_serde::to_vec(&cache_entry)
                .map_err(ChronError::CacheSerializeError)?;
            cache.insert(url.as_str(), entities_bin.as_slice()).map_err(ChronError::CachePutError)?;

            // Immediately fetch again from cache to verify everything is working
            let entities = self.get_cached(&url.as_str())
                .expect("Error getting cache entry immediately after it was saved")
                .expect("Cache entry was not found immediately after it was saved");
            
            Ok(entities)
        };
        
        if let Some(cache) = &self.cache {
            // Fetches are already so slow that cache flushing should be a drop in the bucket. Non-fetch
            // requests shouldn't dirty the cache at all and so this should be near-instant.
            cache.flush().map_err(ChronError::CacheFlushError)?;
        }
        
        result
    }
}
