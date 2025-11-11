pub mod tag;

use sqlx::{Pool, Error, postgres::Postgres};

pub async fn migrate(pool: &Pool<Postgres>) -> Result<(), Error>
{
    sqlx::migrate!("./migrations")
        .run(pool)
        .await?;
    Ok(())
}
