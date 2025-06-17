use http_cache_reqwest::{CACacheManager, Cache, CacheMode, HttpCache, HttpCacheOptions};
use reqwest::Client;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};

pub fn get_caching_http_client() -> ClientWithMiddleware {
    ClientBuilder::new(Client::new())
        .with(Cache(HttpCache {
            mode: CacheMode::ForceCache,
            manager: CACacheManager {
                path: "../http-cacache".into()
            },
            options: HttpCacheOptions::default(),
        }))
        .build()
}
