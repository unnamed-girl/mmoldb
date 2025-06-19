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

#[derive(Deserialize)]
pub struct ChronEntities<EntityT> {
    pub items: Vec<ChronEntity<EntityT>>,
    pub next_page: Option<String>,
}

#[derive(Deserialize)]
pub struct ChronEntity<EntityT> {
    pub kind: String,
    pub entity_id: String,
    pub valid_from: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub data: EntityT,
}

pub enum StreamItem<EntityT> {
    Entity(ChronEntity<EntityT>),
    // This is emitted after every entity from its page has been emitted. The idea is that you can
    // store the most recent page token from CompletedPage and use it to pick up where you left off
    // in a subsequent call to entities_with_page_breaks. Note that if you do this you'll still
    // see duplicates unless you stop processing exactly on a page boundary.
    CompletedPage(String),
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

pub fn entities_with_page_breaks<EntityT: for<'a> Deserialize<'a>>(
    client: &ClientWithMiddleware,
    cache_mode: http_cache_reqwest::CacheMode,
    kind: &str,
    count: usize,
    first_page: Option<String>,
) -> impl Stream<Item = Result<StreamItem<EntityT>, ChronError>> {
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
                            if let Some(next_page) = response.next_page {
                                Some((Ok((response.items, this_page)), State::Continue(Some(next_page))))
                            } else {
                                // No next token means it's the end of the stream, but we still have
                                // to return the item we have. Also, don't pass through this_page 
                                // because that means a completed page. This page might not be 
                                // completed.
                                Some((Ok((response.items, None)), State::End))
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
        .map_ok(|(entities, this_page)| {
            stream::iter(
                entities.into_iter()
                    .map(|entity| Ok(StreamItem::Entity(entity)))
                    .chain(this_page.map(|page| Ok(StreamItem::CompletedPage(page))))
            )
        })
        .try_flatten()
}