use std::{fmt::Display, hash::Hash};

use uuid::Uuid;

pub trait Id: Eq + Hash + Clone + Display {}

pub trait Identifier<T> {
    fn id(&self) -> &T;

    fn name(&self) -> &str;
}

/// object identifier(e.g. tenant/database/user)
pub type Oid = u128;

impl Id for Oid {}

#[async_trait::async_trait]
pub trait OidGenerator {
    async fn next_oid(&self) -> std::result::Result<Oid, String>;
}

#[derive(Default)]
pub struct MemoryOidGenerator {}

#[async_trait::async_trait]
impl OidGenerator for MemoryOidGenerator {
    async fn next_oid(&self) -> std::result::Result<Oid, String> {
        Ok(Uuid::new_v4().as_u128())
    }
}
