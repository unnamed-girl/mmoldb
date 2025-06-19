use chrono::{DateTime, Utc};
use futures::{stream, Stream, TryStreamExt};
use itertools::Itertools;
use reqwest_middleware::ClientWithMiddleware;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChronError {
    #[error(transparent)]
    Network(#[from] reqwest_middleware::Error),
    
    #[error(transparent)]
    Deserialize(#[from] reqwest::Error),
}

// This is exactly like ChronEntities except it contains its own page 
// token instead of the next page's token. The first page's token is
// None
#[derive(Debug, Deserialize)]
pub struct ChronPage<EntityT> {
    pub items: Vec<ChronEntity<EntityT>>,
    pub page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChronEntities<EntityT> {
    pub items: Vec<ChronEntity<EntityT>>,
    pub next_page: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ChronEntity<EntityT> {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub data: EntityT,
}

async fn get_entities_page<EntityT: for<'a> Deserialize<'a>>(
    client: &ClientWithMiddleware,
    cache_mode: http_cache_reqwest::CacheMode,
    kind: &str,
    count: &str,
    page: Option<&str>,
) -> Result<ChronEntities<EntityT>, ChronError> {
    let request = client
        .get("https://freecashe.ws/api/chron/v0/entities")
        .with_extension(cache_mode)
        .query(&[("kind", kind), ("count", &count), ("order", "asc")]);

    let request = if let Some(page_token) = page {
        request.query(&[("page", page_token)])
    } else {
        request
    };

    let response = request.send().await?.json().await?;

    Ok(response)
}

pub fn entity_pages<EntityT: for<'a> Deserialize<'a>>(
    client: &ClientWithMiddleware,
    cache_mode: http_cache_reqwest::CacheMode,
    kind: &str,
    count: usize,
    first_page: Option<String>,
) -> impl Stream<Item = Result<ChronPage<EntityT>, ChronError>> {
    enum State {
        Continue(Option<String>),
        End,
    }

    let count = count.to_string();
    stream::unfold(State::Continue(first_page), move |state| {
        let count = count.clone();
        async move {
            match state {
                State::Continue(this_page) => {
                    match get_entities_page(client, cache_mode, kind, &count, this_page.as_deref()).await {
                        Ok(response) => {
                            let page = ChronPage {
                                items: response.items,
                                page_token: this_page,
                            };
                            
                            if let Some(next_page) = response.next_page {
                                Some((Ok(page), State::Continue(Some(next_page))))
                            } else {
                                // No next token means it's the end of the stream, but we still have
                                // to return the item we have.
                                Some((Ok(page), State::End))
                            }
                        }
                        Err(err) => {
                            // Yield the error once and then end
                            Some((Err(err), State::End))
                        }
                    }
                }
                State::End => {
                    None
                }
            }
        }
    })
}