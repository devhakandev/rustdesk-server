use async_trait::async_trait;
use hbb_common::{log, ResultType};
use sqlx::{
    connection::{ConnectOptions, Connection},
    error::Error as SqlxError,
    query::query,
    row::Row,
};
use sqlx_sqlite::{Sqlite, SqliteConnectOptions, SqliteConnection};
use std::{ops::DerefMut, str::FromStr};
//use sqlx::postgres::PgPoolOptions;
//use sqlx::mysql::MySqlPoolOptions;

type Pool = deadpool::managed::Pool<DbPool>;

pub struct DbPool {
    url: String,
}

#[async_trait]
impl deadpool::managed::Manager for DbPool {
    type Type = SqliteConnection;
    type Error = SqlxError;
    async fn create(&self) -> Result<SqliteConnection, SqlxError> {
        let opt = SqliteConnectOptions::from_str(&self.url)
            .unwrap()
            .log_statements(log::LevelFilter::Debug);
        SqliteConnection::connect_with(&opt).await
    }
    async fn recycle(
        &self,
        obj: &mut SqliteConnection,
    ) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

#[derive(Clone)]
pub struct Database {
    pool: Pool,
}

#[derive(Default)]
pub struct Peer {
    pub guid: Vec<u8>,
    pub id: String,
    pub uuid: Vec<u8>,
    pub pk: Vec<u8>,
    pub user: Option<Vec<u8>>,
    pub info: String,
    pub status: Option<i64>,
}

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        if !std::path::Path::new(url).exists() {
            std::fs::File::create(url).ok();
        }
        let n: usize = std::env::var("MAX_DATABASE_CONNECTIONS")
            .unwrap_or_else(|_| "1".to_owned())
            .parse()
            .unwrap_or(1);
        log::debug!("MAX_DATABASE_CONNECTIONS={}", n);
        let pool = Pool::new(
            DbPool {
                url: url.to_owned(),
            },
            n,
        );
        let _ = pool.get().await?; // test
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> ResultType<()> {
        let mut conn = self.pool.get().await?;
        for statement in [
            r#"
            create table if not exists peer (
                guid blob primary key not null,
                id varchar(100) not null,
                uuid blob not null,
                pk blob not null,
                created_at datetime not null default(current_timestamp),
                updated_at datetime not null default(current_timestamp),
                user blob,
                status tinyint,
                note varchar(300),
                info text not null
            ) without rowid;
            "#,
            "create unique index if not exists index_peer_id on peer (id);",
            "create index if not exists index_peer_user on peer (user);",
            "create index if not exists index_peer_created_at on peer (created_at);",
            "create index if not exists index_peer_status on peer (status);",
        ] {
            query::<Sqlite>(statement).execute(conn.deref_mut()).await?;
        }

        // Existing OSS databases do not have updated_at. SQLite cannot add this
        // column with CURRENT_TIMESTAMP as a non-constant default, so add it
        // nullable and backfill it for admin-panel heartbeat reads.
        let has_updated_at = query::<Sqlite>("select updated_at from peer limit 1")
            .fetch_optional(conn.deref_mut())
            .await
            .is_ok();
        if !has_updated_at {
            query::<Sqlite>("alter table peer add column updated_at datetime")
                .execute(conn.deref_mut())
                .await
                .ok();
            query::<Sqlite>(
                "update peer set updated_at = coalesce(updated_at, created_at, current_timestamp)",
            )
            .execute(conn.deref_mut())
            .await
            .ok();
        }
        Ok(())
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        let row =
            query::<Sqlite>("select guid, id, uuid, pk, user, status, info from peer where id = ?")
                .bind(id)
                .fetch_optional(self.pool.get().await?.deref_mut())
                .await?;
        if let Some(row) = row {
            Ok(Some(Peer {
                guid: row.try_get("guid")?,
                id: row.try_get("id")?,
                uuid: row.try_get("uuid")?,
                pk: row.try_get("pk")?,
                user: row.try_get("user")?,
                status: row.try_get("status")?,
                info: row.try_get("info")?,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        query::<Sqlite>("insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)")
            .bind(&guid)
            .bind(id)
            .bind(uuid)
            .bind(pk)
            .bind(info)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        self.touch_peer(id).await?;
        Ok(guid)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        query::<Sqlite>("update peer set id=?, pk=?, info=? where guid=?")
            .bind(id)
            .bind(pk)
            .bind(info)
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        self.touch_peer(id).await?;
        Ok(())
    }

    pub async fn touch_peer(&self, id: &str) -> ResultType<()> {
        query::<Sqlite>("update peer set updated_at = current_timestamp where id = ?")
            .bind(id)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hbb_common::tokio;
    #[test]
    fn test_insert() {
        insert();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn insert() {
        let db = super::Database::new("test.sqlite3").await.unwrap();
        let mut jobs = vec![];
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                let empty_vec = Vec::new();
                cloned
                    .insert_peer(&id, &empty_vec, &empty_vec, "")
                    .await
                    .unwrap();
            });
            jobs.push(a);
        }
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                cloned.get_peer(&id).await.unwrap();
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }
}
