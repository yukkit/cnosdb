use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::Arc,
};

use parking_lot::RwLock;

use crate::oid::{Id, Identifier};

use super::{
    privilege::{DatabasePrivilege, GlobalPrivilege, Privilege, TenantObjectPrivilege},
    Result,
};

pub enum UserRole<T> {
    // 拥有对整个数据库实例的最高级权限
    // 支持用户管理和组织管理，但是不能管理组织中的内容
    Dba,
    // tenantId -> rogRole
    Public(HashMap<T, TenantRole<T>>),
}

impl<T: Id> UserRole<T> {
    pub fn to_privileges(&self) -> HashSet<Privilege<T>> {
        match self {
            Self::Dba => vec![
                Privilege::Global(GlobalPrivilege::Tenant(None)),
                Privilege::Global(GlobalPrivilege::User(None)),
                Privilege::TenantObject(TenantObjectPrivilege::MemberFull, None),
                Privilege::TenantObject(TenantObjectPrivilege::RoleFull, None),
                Privilege::TenantObject(
                    TenantObjectPrivilege::Database(DatabasePrivilege::Full, None),
                    None,
                ),
            ]
            .into_iter()
            .collect(),
            Self::Public(roles) => roles
                .iter()
                .map(|(tenant_id, tenant_role)| tenant_role.to_privileges(tenant_id))
                .reduce(|a, b| a.union(&b).cloned().collect())
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TenantRoleIdentifier {
    System(SystemTenantRole),
    Custom(String),
}

#[derive(Debug, Clone)]
pub enum TenantRole<T> {
    System(SystemTenantRole),
    Custom(CustomTenantRoleRef<T>),
}

impl<T: Id> TenantRole<T> {
    pub fn to_privileges(&self, tenant_id: &T) -> HashSet<Privilege<T>> {
        match self {
            Self::System(e) => e.to_privileges(tenant_id),
            Self::Custom(e) => e.read().to_privileges(tenant_id),
        }
    }
}

impl<T> Eq for TenantRole<T> where T: Eq {}

impl<T> PartialEq for TenantRole<T>
where
    T: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::System(l0), Self::System(r0)) => l0 == r0,
            (Self::Custom(l0), Self::Custom(r0)) => l0.read().id() == r0.read().id(),
            (_, _) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SystemTenantRole {
    Owner,
    Member,
}

impl TryFrom<&str> for SystemTenantRole {
    type Error = String;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "owner" => Ok(Self::Owner),
            "member" => Ok(Self::Member),
            _ => Err(format!("Expected [owner,member], found {}", value)),
        }
    }
}

impl SystemTenantRole {
    pub fn to_privileges<T>(&self, tenant_id: &T) -> HashSet<Privilege<T>>
    where
        T: Id,
    {
        match self {
            Self::Owner => vec![
                Privilege::Global(GlobalPrivilege::Tenant(Some(tenant_id.clone()))),
                Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(tenant_id.clone())),
                Privilege::TenantObject(TenantObjectPrivilege::RoleFull, Some(tenant_id.clone())),
                Privilege::TenantObject(
                    TenantObjectPrivilege::Database(DatabasePrivilege::Full, None),
                    Some(tenant_id.clone()),
                ),
            ]
            .into_iter()
            .collect(),
            Self::Member => vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(DatabasePrivilege::Read, None),
                Some(tenant_id.clone()),
            )]
            .into_iter()
            .collect(),
        }
    }
}

pub type CustomTenantRoleRef<T> = Arc<RwLock<CustomTenantRole<T>>>;

#[derive(Debug, Clone)]
pub struct CustomTenantRole<T> {
    id: T,
    name: String,
    system_role: SystemTenantRole,
    // database_name -> privileges
    // only add database privilege
    additiona_privileges: HashMap<String, DatabasePrivilege>,
}

impl<T> CustomTenantRole<T> {
    pub fn new(
        id: T,
        name: String,
        system_role: SystemTenantRole,
        // database_name -> privileges
        // only add database privilege
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> Self {
        Self {
            id,
            name,
            system_role,
            additiona_privileges,
        }
    }
}

impl<T: Id> CustomTenantRole<T> {
    pub fn to_privileges(&self, tenant_id: &T) -> HashSet<Privilege<T>> {
        let privileges = self.system_role.to_privileges(tenant_id);

        let additiona_privileges = self
            .additiona_privileges
            .iter()
            .map(|(db_name, privilege)| {
                Privilege::TenantObject(
                    TenantObjectPrivilege::Database(privilege.clone(), Some(db_name.clone())),
                    Some(tenant_id.clone()),
                )
            })
            .collect::<HashSet<Privilege<T>>>();

        privileges.union(&additiona_privileges).cloned().collect()
    }

    pub fn grant_privilege(
        &mut self,
        database_name: String,
        privilege: DatabasePrivilege,
    ) -> Result<()> {
        self.additiona_privileges.insert(database_name, privilege);

        Ok(())
    }

    pub fn revoke_privilege(
        &mut self,
        database_name: &str,
        privilege: &DatabasePrivilege,
    ) -> Result<bool> {
        if let Some(p) = self.additiona_privileges.get(database_name) {
            if p == privilege {
                Ok(self.additiona_privileges.remove(database_name).is_some())
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

impl<T> Identifier<T> for CustomTenantRole<T> {
    fn id(&self) -> &T {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}
