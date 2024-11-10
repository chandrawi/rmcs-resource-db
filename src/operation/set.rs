use sqlx::{Pool, Row, Error};
use sqlx::postgres::{Postgres, PgRow};
use sea_query::{PostgresQueryBuilder, Query, Expr, Order};
use sea_query_binder::SqlxBinder;
use uuid::Uuid;

use crate::schema::set::{Set, SetMap, SetTemplate, SetTemplateMap, SetSchema, SetMember, SetTemplateSchema, SetTemplateMember};

pub(crate) async fn select_set(pool: &Pool<Postgres>, 
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    template_id: Option<Uuid>,
    name: Option<&str>
) -> Result<Vec<SetSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (Set::Table, Set::SetId),
            (Set::Table, Set::TemplateId),
            (Set::Table, Set::Name),
            (Set::Table, Set::Description)
        ])
        .columns([
            (SetMap::Table, SetMap::DeviceId),
            (SetMap::Table, SetMap::ModelId),
            (SetMap::Table, SetMap::DataIndex)
        ])
        .from(Set::Table)
        .left_join(SetMap::Table, 
            Expr::col((Set::Table, Set::SetId))
            .equals((SetMap::Table, SetMap::SetId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((Set::Table, Set::SetId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((Set::Table, Set::SetId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(template_id) = template_id {
            stmt = stmt.and_where(Expr::col((Set::Table, Set::TemplateId)).eq(template_id)).to_owned();
        }
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((Set::Table, Set::Name)).like(name_like)).to_owned();
        }
    }

    let (sql, values) = stmt
        .order_by((Set::Table, Set::SetId), Order::Asc)
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut last_id: Option<Uuid> = None;
    let mut set_schema_vec: Vec<SetSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // get last set_schema in set_schema_vec or default
            let mut set_schema = set_schema_vec.pop().unwrap_or_default();
            // on every new id found insert set_schema to set_schema_vec
            let id: Uuid = row.get(0);
            if let Some(value) = last_id {
                if value != id {
                    set_schema_vec.push(set_schema.clone());
                    set_schema = SetSchema::default();
                }
            }
            last_id = Some(id);
            set_schema.id = id;
            set_schema.template_id = row.get(1);
            set_schema.name = row.get(2);
            set_schema.description = row.get(3);
            // update set_schema members if non empty member found
            let id: Result<Uuid, Error> = row.try_get(4);
            if let Ok(device_id) = id {
                set_schema.members.push(SetMember {
                    device_id,
                    model_id: row.try_get(5).unwrap_or_default(),
                    data_index: row.try_get(6).unwrap_or_default()
                });
            }
            // update set_schema_vec with updated set_schema
            set_schema_vec.push(set_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(set_schema_vec)
}

pub(crate) async fn insert_set(pool: &Pool<Postgres>,
    id: Uuid,
    template_id: Uuid,
    name: &str,
    description: Option<&str>,
) -> Result<Uuid, Error>
{
    let (sql, values) = Query::insert()
        .into_table(Set::Table)
        .columns([
            Set::SetId,
            Set::TemplateId,
            Set::Name,
            Set::Description
        ])
        .values([
            id.into(),
            template_id.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_set(pool: &Pool<Postgres>,
    id: Uuid,
    template_id: Option<Uuid>,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(Set::Table)
        .to_owned();

    if let Some(value) = template_id {
        stmt = stmt.value(Set::TemplateId, value).to_owned();
    }
    if let Some(value) = name {
        stmt = stmt.value(Set::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(Set::Description, value).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(Set::SetId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_set(pool: &Pool<Postgres>, 
    id: Uuid
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(Set::Table)
        .and_where(Expr::col(Set::SetId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn read_set_members(pool: &Pool<Postgres>, 
    set_id: Uuid
) -> Result<Vec<SetMember>, Error>
{
    let (sql, values) = Query::select()
        .columns([
            (SetMap::Table, SetMap::DeviceId),
            (SetMap::Table, SetMap::ModelId),
            (SetMap::Table, SetMap::DataIndex),
        ])
        .from(SetMap::Table)
        .and_where(Expr::col(SetMap::SetId).eq(set_id))
        .order_by((SetMap::Table, SetMap::SetPosition), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);
    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            SetMember {
                device_id: row.try_get(0).unwrap_or_default(),
                model_id: row.try_get(1).unwrap_or_default(),
                data_index: row.try_get(2).unwrap_or_default()
            }
        })
        .fetch_all(pool)
        .await
}

async fn update_set_position_number(pool: &Pool<Postgres>,
    set_id: Uuid,
    device_id: Uuid,
    model_id: Uuid,
    position: Option<usize>,
    number: Option<usize>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(SetMap::Table)
        .to_owned();
    if let Some(pos) = position {
        stmt = stmt
            .value(SetMap::SetPosition, pos as i16)
            .and_where(Expr::col(SetMap::DeviceId).eq(device_id))
            .and_where(Expr::col(SetMap::ModelId).eq(model_id))
            .to_owned();
    }
    if let Some(num) = number {
        stmt = stmt.value(SetMap::SetNumber, num as i16).to_owned();
    }
    let (sql, values) = stmt
        .and_where(Expr::col(SetMap::SetId).eq(set_id))
        .build_sqlx(PostgresQueryBuilder);
    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn insert_set_member(pool: &Pool<Postgres>,
    id: Uuid,
    device_id: Uuid,
    model_id: Uuid,
    data_index: &[u8]
) -> Result<(), Error>
{
    // get members of the set then calculate new data position and data number
    let set_members = read_set_members(pool, id).await?;
    let position = set_members.iter().fold(0, |acc, e| acc + e.data_index.len());
    let number = position + data_index.len();

    let (sql, values) = Query::insert()
        .into_table(SetMap::Table)
        .columns([
            SetMap::SetId,
            SetMap::DeviceId,
            SetMap::ModelId,
            SetMap::DataIndex,
            SetMap::SetPosition,
            SetMap::SetNumber
        ])
        .values([
            id.into(),
            device_id.into(),
            model_id.into(),
            data_index.to_owned().into(),
            (position as i16).into(),
            (number as i16).into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    // update data number of all set members
    update_set_position_number(pool, id, device_id, model_id, None, Some(number)).await?;

    Ok(())
}

pub(crate) async fn delete_set_member(pool: &Pool<Postgres>,
    id: Uuid,
    device_id: Uuid,
    model_id: Uuid
) -> Result<(), Error>
{
    // get members of the set then get index position of deleted set member
    let set_members = read_set_members(pool, id).await?;
    let index = set_members.iter().position(|e| e.device_id == device_id && e.model_id == model_id);

    let (sql, values) = Query::delete()
        .from_table(SetMap::Table)
        .and_where(Expr::col(SetMap::SetId).eq(id))
        .and_where(Expr::col(SetMap::DeviceId).eq(device_id))
        .and_where(Expr::col(SetMap::ModelId).eq(model_id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    if let Some(idx) = index {
        // calculate data number then update data number of all set members
        let number = set_members.iter().fold(0, |acc, e| acc + e.data_index.len()) - set_members[idx].data_index.len();
        update_set_position_number(pool, id, device_id, model_id, None, Some(number)).await?;
        // update data position of members with index position after deleted set member
        let mut position = 0;
        for (i, member) in set_members.iter().enumerate() {
            if i > idx {
                update_set_position_number(pool, id, member.device_id, member.model_id, Some(position), None).await?;
            }
            position += member.data_index.len();
        }
    }

    Ok(())
}

pub(crate) async fn swap_set_member(pool: &Pool<Postgres>,
    id: Uuid,
    device_id_1: Uuid,
    model_id_1: Uuid,
    device_id_2: Uuid,
    model_id_2: Uuid
) -> Result<(), Error>
{
    // get members of the set then get index positions
    let mut set_members = read_set_members(pool, id).await?;
    let index_1 = set_members.iter().position(|e| e.device_id == device_id_1 && e.model_id == model_id_1);
    let index_2 = set_members.iter().position(|e| e.device_id == device_id_2 && e.model_id == model_id_2);

    // swap position index
    if let (Some(i1), Some(i2)) = (index_1, index_2) {
        set_members.swap(i1, i2);
        // update data position of members
        let mut position = 0;
        for (i, member) in set_members.iter().enumerate() {
            if i >= i1 || i >= i2 {
                update_set_position_number(pool, id, member.device_id, member.model_id, Some(position), None).await?;
            }
            position += member.data_index.len();
        }
    }

    Ok(())
}

pub(crate) async fn select_set_template(pool: &Pool<Postgres>, 
    id: Option<Uuid>,
    ids: Option<&[Uuid]>,
    name: Option<&str>
) -> Result<Vec<SetTemplateSchema>, Error>
{
    let mut stmt = Query::select()
        .columns([
            (SetTemplate::Table, SetTemplate::TemplateId),
            (SetTemplate::Table, SetTemplate::Name),
            (SetTemplate::Table, SetTemplate::Description)
        ])
        .columns([
            (SetTemplateMap::Table, SetTemplateMap::TypeId),
            (SetTemplateMap::Table, SetTemplateMap::ModelId),
            (SetTemplateMap::Table, SetTemplateMap::DataIndex)
        ])
        .from(SetTemplate::Table)
        .left_join(SetTemplateMap::Table, 
            Expr::col((SetTemplate::Table, SetTemplate::TemplateId))
            .equals((SetTemplateMap::Table, SetTemplateMap::TemplateId))
        )
        .to_owned();

    if let Some(id) = id {
        stmt = stmt.and_where(Expr::col((SetTemplate::Table, SetTemplate::TemplateId)).eq(id)).to_owned();
    }
    else if let Some(ids) = ids {
        stmt = stmt.and_where(Expr::col((SetTemplate::Table, SetTemplate::TemplateId)).is_in(ids.to_vec())).to_owned();
    }
    else {
        if let Some(name) = name {
            let name_like = String::from("%") + name + "%";
            stmt = stmt.and_where(Expr::col((SetTemplate::Table, SetTemplate::Name)).like(name_like)).to_owned();
        }
    }

    let (sql, values) = stmt
        .order_by((SetTemplate::Table, SetTemplate::TemplateId), Order::Asc)
        .order_by((SetTemplateMap::Table, SetTemplateMap::TemplateIndex), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    let mut last_id: Option<Uuid> = None;
    let mut template_schema_vec: Vec<SetTemplateSchema> = Vec::new();

    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            // get last template_schema in template_schema_vec or default
            let mut template_schema = template_schema_vec.pop().unwrap_or_default();
            // on every new id found insert template_schema to template_schema_vec
            let id: Uuid = row.get(0);
            if let Some(value) = last_id {
                if value != id {
                    template_schema_vec.push(template_schema.clone());
                    template_schema = SetTemplateSchema::default();
                }
            }
            last_id = Some(id);
            template_schema.id = id;
            template_schema.name = row.get(1);
            template_schema.description = row.get(2);
            // update template_schema members if non empty member found
            let id: Result<Uuid, Error> = row.try_get(3);
            if let Ok(type_id) = id {
                template_schema.members.push(SetTemplateMember {
                    type_id,
                    model_id: row.try_get(4).unwrap_or_default(),
                    data_index: row.try_get(5).unwrap_or_default()
                });
            }
            // update template_schema_vec with updated template_schema
            template_schema_vec.push(template_schema.clone());
        })
        .fetch_all(pool)
        .await?;

    Ok(template_schema_vec)
}

pub(crate) async fn insert_set_template(pool: &Pool<Postgres>,
    id: Uuid,
    name: &str,
    description: Option<&str>,
) -> Result<Uuid, Error>
{
    let (sql, values) = Query::insert()
        .into_table(SetTemplate::Table)
        .columns([
            SetTemplate::TemplateId,
            SetTemplate::Name,
            SetTemplate::Description
        ])
        .values([
            id.into(),
            name.into(),
            description.unwrap_or_default().into()
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(id)
}

pub(crate) async fn update_set_template(pool: &Pool<Postgres>,
    id: Uuid,
    name: Option<&str>,
    description: Option<&str>
) -> Result<(), Error>
{
    let mut stmt = Query::update()
        .table(SetTemplate::Table)
        .to_owned();

    if let Some(value) = name {
        stmt = stmt.value(SetTemplate::Name, value).to_owned();
    }
    if let Some(value) = description {
        stmt = stmt.value(SetTemplate::Description, value).to_owned();
    }

    let (sql, values) = stmt
        .and_where(Expr::col(SetTemplate::TemplateId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_set_template(pool: &Pool<Postgres>, 
    id: Uuid
) -> Result<(), Error> 
{
    let (sql, values) = Query::delete()
        .from_table(SetTemplate::Table)
        .and_where(Expr::col(SetTemplate::TemplateId).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

async fn read_set_template_members(pool: &Pool<Postgres>, 
    template_id: Uuid
) -> Result<Vec<SetTemplateMember>, Error>
{
    let (sql, values) = Query::select()
        .columns([
            (SetTemplateMap::Table, SetTemplateMap::TypeId),
            (SetTemplateMap::Table, SetTemplateMap::ModelId),
            (SetTemplateMap::Table, SetTemplateMap::DataIndex)
        ])
        .from(SetTemplateMap::Table)
        .and_where(Expr::col(SetTemplateMap::TemplateId).eq(template_id))
        .order_by((SetTemplateMap::Table, SetTemplateMap::TemplateIndex), Order::Asc)
        .build_sqlx(PostgresQueryBuilder);
    sqlx::query_with(&sql, values)
        .map(|row: PgRow| {
            SetTemplateMember {
                type_id: row.try_get(0).unwrap_or_default(),
                model_id: row.try_get(1).unwrap_or_default(),
                data_index: row.try_get(2).unwrap_or_default()
            }
        })
        .fetch_all(pool)
        .await
}

async fn update_set_template_index(pool: &Pool<Postgres>, 
    template_id: Uuid, 
    index: usize, 
    new_index: usize
) -> Result<(), Error>
{
    let (sql, values) = Query::update()
        .table(SetTemplateMap::Table)
        .value(SetTemplateMap::TemplateIndex, new_index as i16)
        .and_where(Expr::col(SetTemplateMap::TemplateId).eq(template_id))
        .and_where(Expr::col(SetTemplateMap::TemplateIndex).eq(index as i16))
        .build_sqlx(PostgresQueryBuilder);
    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn insert_set_template_member(pool: &Pool<Postgres>,
    id: Uuid,
    type_id: Uuid,
    model_id: Uuid,
    data_index: &[u8]
) -> Result<(), Error>
{
    // get members of the set template then calculate new template index
    let template_members = read_set_template_members(pool, id).await?;
    let new_index = template_members.len() as i16;

    let (sql, values) = Query::insert()
        .into_table(SetTemplateMap::Table)
        .columns([
            SetTemplateMap::TemplateId,
            SetTemplateMap::TypeId,
            SetTemplateMap::ModelId,
            SetTemplateMap::DataIndex,
            SetTemplateMap::TemplateIndex
        ])
        .values([
            id.into(),
            type_id.into(),
            model_id.into(),
            data_index.to_owned().into(),
            new_index.into() // make sure index is greatest among other map
        ])
        .unwrap_or(&mut sea_query::InsertStatement::default())
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    Ok(())
}

pub(crate) async fn delete_set_template_member(pool: &Pool<Postgres>,
    id: Uuid,
    template_index: usize
) -> Result<(), Error>
{
    // get members of the set template
    let template_members = read_set_template_members(pool, id).await?;

    let (sql, values) = Query::delete()
        .from_table(SetTemplateMap::Table)
        .and_where(Expr::col(SetTemplateMap::TemplateId).eq(id))
        .and_where(Expr::col(SetTemplateMap::TemplateIndex).eq(template_index as i16))
        .build_sqlx(PostgresQueryBuilder);

    sqlx::query_with(&sql, values)
        .execute(pool)
        .await?;

    // update template index after deleted member
    for i in 0..template_members.len() {
        if i > template_index {
            update_set_template_index(pool, id, i, i - 1).await?;
        }
    }

    Ok(())
}

pub(crate) async fn swap_set_template_member(pool: &Pool<Postgres>,
    id: Uuid,
    template_index_1: usize,
    template_index_2: usize
) -> Result<(), Error>
{
    // update data position and data number
    update_set_template_index(pool, id, template_index_1, i16::MAX as usize).await?;
    update_set_template_index(pool, id, template_index_2, template_index_1).await?;
    update_set_template_index(pool, id, i16::MAX as usize, template_index_2).await?;

    Ok(())
}
