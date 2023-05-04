use sqlx::{Pool, Row, Error};
use sqlx::mysql::{MySql, MySqlRow};
use sea_query::{MysqlQueryBuilder, Query, Expr, Func};
use sea_query_binder::SqlxBinder;

use crate::schema::group::{GroupModel, GroupModelMap, GroupDevice, GroupDeviceMap, GroupKind, GroupSchema};

enum GroupSelector {
    Id(u32),
    Name(String),
    Category(String),
    NameCategory(String, String)
}

async fn select_group(pool: &Pool<MySql>, 
    kind: GroupKind,
    selector: GroupSelector
) -> Result<Vec<GroupSchema>, Error>
{
    let mut stmt = Query::select().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .columns([
                    (GroupModel::Table, GroupModel::GroupId),
                    (GroupModel::Table, GroupModel::Name),
                    (GroupModel::Table, GroupModel::Category),
                    (GroupModel::Table, GroupModel::Description)
                ])
                .columns([
                    (GroupModelMap::Table, GroupModelMap::ModelId)
                ])
                .from(GroupModel::Table)
                .left_join(GroupModelMap::Table, 
                    Expr::col((GroupModel::Table, GroupModel::GroupId))
                    .equals((GroupModelMap::Table, GroupModelMap::GroupId))
                )
                .to_owned();
            match selector {
                GroupSelector::Id(id) => {
                    stmt = stmt.and_where(Expr::col((GroupModel::Table, GroupModel::GroupId)).eq(id)).to_owned();
                },
                GroupSelector::Name(name) => {
                    stmt = stmt.and_where(Expr::col((GroupModel::Table, GroupModel::Name)).like(name)).to_owned();
                },
                GroupSelector::Category(category) => {
                    stmt = stmt.and_where(Expr::col((GroupModel::Table, GroupModel::Category)).eq(category)).to_owned();
                },
                GroupSelector::NameCategory(name, category) => {
                    stmt = stmt
                        .and_where(Expr::col((GroupModel::Table, GroupModel::Name)).like(name))
                        .and_where(Expr::col((GroupModel::Table, GroupModel::Category)).eq(category))
                        .to_owned();
                }
            }
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .columns([
                    (GroupDevice::Table, GroupDevice::GroupId),
                    (GroupDevice::Table, GroupDevice::Name),
                    (GroupDevice::Table, GroupDevice::Category),
                    (GroupDevice::Table, GroupDevice::Description)
                ])
                .columns([
                    (GroupDeviceMap::Table, GroupDeviceMap::DeviceId)
                ])
                .from(GroupDevice::Table)
                .left_join(GroupDeviceMap::Table, 
                    Expr::col((GroupDevice::Table, GroupDevice::GroupId))
                    .equals((GroupDeviceMap::Table, GroupDeviceMap::GroupId))
                )
                .and_where(Expr::col((GroupDevice::Table, GroupDevice::Kind)).eq(kind.to_string())).to_owned()
                .to_owned();
            match selector {
                GroupSelector::Id(id) => {
                    stmt = stmt.and_where(Expr::col((GroupDevice::Table, GroupDevice::GroupId)).eq(id)).to_owned();
                },
                GroupSelector::Name(name) => {
                    stmt = stmt.and_where(Expr::col((GroupDevice::Table, GroupDevice::Name)).like(name)).to_owned();
                },
                GroupSelector::Category(category) => {
                    stmt = stmt.and_where(Expr::col((GroupDevice::Table, GroupDevice::Category)).eq(category)).to_owned();
                },
                GroupSelector::NameCategory(name, category) => {
                    stmt = stmt
                        .and_where(Expr::col((GroupDevice::Table, GroupDevice::Name)).like(name))
                        .and_where(Expr::col((GroupDevice::Table, GroupDevice::Category)).eq(category))
                        .to_owned();
                }
            }
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    let mut id_vec: Vec<u32> = Vec::new();
    let mut group_schema: GroupSchema = GroupSchema::default();
    let mut group_schema_vec: Vec<GroupSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: MySqlRow| {
            let group_id: u32 = row.get(0);
            let name: String = row.get(1);
            let category: String = row.get(2);
            let description: String = row.get(3);
            let member_id: Result<u64, Error> = row.try_get(4);

            // on every new group_id found add id_vec and update group_schema scalar member
            if id_vec.iter().filter(|el| **el == group_id).count() == 0 {
                id_vec.push(group_id);
                group_schema.id = group_id;
                group_schema.name = name;
                group_schema.category = category;
                group_schema.description = description;
                // insert new group_schema to group_schema_vec
                group_schema_vec.push(group_schema.clone());
            }
            // update group_schema if non empty member_id found
            if let Ok(value) = member_id {
                group_schema.members.push(value);
            }
            // update group_schema_vec with updated group_schema
            group_schema_vec.pop();
            group_schema_vec.push(group_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(group_schema_vec)
}

pub(crate) async fn select_group_by_id(pool: &Pool<MySql>,
    kind: GroupKind,
    id: u32
) -> Result<GroupSchema, Error>
{
    let results = select_group(pool, kind, GroupSelector::Id(id)).await?;
    match results.into_iter().next() {
        Some(value) => Ok(value),
        None => Err(Error::RowNotFound)
    }
}

pub(crate) async fn select_group_by_name(pool: &Pool<MySql>,
    kind: GroupKind,
    name: &str
) -> Result<Vec<GroupSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_group(pool, kind, GroupSelector::Name(name_like)).await
}

pub(crate) async fn select_group_by_category(pool: &Pool<MySql>,
    kind: GroupKind,
    category: &str
) -> Result<Vec<GroupSchema>, Error>
{
    select_group(pool, kind, GroupSelector::Category(String::from(category))).await
}

pub(crate) async fn select_group_by_name_category(pool: &Pool<MySql>,
    kind: GroupKind,
    name: &str,
    category: &str
) -> Result<Vec<GroupSchema>, Error>
{
    let name_like = String::from("%") + name + "%";
    select_group(pool, kind, GroupSelector::NameCategory(name_like, String::from(category))).await
}

pub(crate) async fn insert_group(pool: &Pool<MySql>,
    kind: GroupKind,
    name: &str,
    category: &str,
    description: Option<&str>
) -> Result<u32, Error>
{
    let mut stmt = Query::insert().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .into_table(GroupModel::Table)
                .columns([
                    GroupModel::Name,
                    GroupModel::Category,
                    GroupModel::Description
                ])
                .values([
                    name.into(),
                    category.into(),
                    description.unwrap_or_default().into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .into_table(GroupDevice::Table)
                .columns([
                    GroupDevice::Name,
                    GroupDevice::Kind,
                    GroupDevice::Category,
                    GroupDevice::Description
                ])
                .values([
                    name.into(),
                    kind.to_string().into(),
                    category.into(),
                    description.unwrap_or_default().into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    let mut stmt = Query::select().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .expr(Func::max(Expr::col(GroupModel::GroupId)))
                .from(GroupModel::Table)
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .expr(Func::max(Expr::col(GroupDevice::GroupId)))
                .from(GroupDevice::Table)
                .to_owned();
        }
    }
    let sql = stmt.to_string(MysqlQueryBuilder);
    let id: u32 = sqlx::query(&sql)
        .map(|row: MySqlRow| row.get(0))
        .fetch_one(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_group(pool: &Pool<MySql>,
    kind: GroupKind,
    id: u32,
    name: Option<&str>,
    category: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt.table(GroupModel::Table).to_owned();
            if let Some(value) = name {
                stmt = stmt.value(GroupModel::Name, value).to_owned();
            }
            if let Some(value) = category {
                stmt = stmt.value(GroupModel::Category, value).to_owned();
            }
            if let Some(value) = description {
                stmt = stmt.value(GroupModel::Description, value).to_owned();
            }
            stmt = stmt.and_where(Expr::col(GroupModel::GroupId).eq(id)).to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt.table(GroupDevice::Table).to_owned();
            if let Some(value) = name {
                stmt = stmt.value(GroupDevice::Name, value).to_owned();
            }
            if let Some(value) = category {
                stmt = stmt.value(GroupDevice::Category, value).to_owned();
            }
            if let Some(value) = description {
                stmt = stmt.value(GroupDevice::Description, value).to_owned();
            }
            stmt = stmt.and_where(Expr::col(GroupDevice::GroupId).eq(id)).to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_group(pool: &Pool<MySql>, 
    kind: GroupKind,
    id: u32
) -> Result<(), Error> 
{
    let mut stmt = Query::delete().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .from_table(GroupModel::Table)
                .and_where(Expr::col(GroupModel::GroupId).eq(id))
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .from_table(GroupDevice::Table)
                .and_where(Expr::col(GroupDevice::GroupId).eq(id))
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn insert_group_map(pool: &Pool<MySql>,
    kind: GroupKind,
    id: u32,
    member_id: u64
) -> Result<(), Error>
{
    let mut stmt = Query::insert().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .into_table(GroupModelMap::Table)
                .columns([
                    GroupModelMap::GroupId,
                    GroupModelMap::ModelId
                ])
                .values([
                    id.into(),
                    u32::from(member_id as u32).into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .into_table(GroupDeviceMap::Table)
                .columns([
                    GroupDeviceMap::GroupId,
                    GroupDeviceMap::DeviceId
                ])
                .values([
                    id.into(),
                    member_id.into()
                ])
                .unwrap_or(&mut sea_query::InsertStatement::default())
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_group_map(pool: &Pool<MySql>, 
    kind: GroupKind,
    id: u32,
    member_id: u64
) -> Result<(), Error> 
{
    let mut stmt = Query::delete().to_owned();
    match &kind {
        GroupKind::Model => {
            stmt = stmt
                .from_table(GroupModelMap::Table)
                .and_where(Expr::col(GroupModelMap::GroupId).eq(id))
                .and_where(Expr::col(GroupModelMap::ModelId).eq(u32::from(member_id as u32)))
                .to_owned();
        },
        GroupKind::Device | GroupKind::Gateway => {
            stmt = stmt
                .from_table(GroupDeviceMap::Table)
                .and_where(Expr::col(GroupDeviceMap::GroupId).eq(id))
                .and_where(Expr::col(GroupDeviceMap::DeviceId).eq(member_id))
                .to_owned();
        }
    }
    let (sql, values) = stmt.build_sqlx(MysqlQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}
