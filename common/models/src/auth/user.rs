use std::{collections::HashSet, fmt::Display};

use derive_builder::Builder;

use crate::oid::{Identifier, Oid};

use super::{
    privilege::{Privilege, PrivilegeChecker},
    rsa_utils, AuthError, Result,
};

#[derive(Debug, Clone)]
pub struct User {
    desc: UserDesc,
    privileges: HashSet<Privilege<Oid>>,
}

impl User {
    pub fn new(desc: UserDesc, privileges: HashSet<Privilege<Oid>>) -> Self {
        Self { desc, privileges }
    }

    pub fn desc(&self) -> &UserDesc {
        &self.desc
    }

    pub fn check_privilege(&self, privilege: &Privilege<Oid>) -> bool {
        self.privileges.iter().any(|e| e.check_privilege(privilege))
    }
}

#[derive(Debug, Clone)]
pub struct UserDesc {
    id: Oid,
    // ident
    name: String,
    options: UserOptions,
}

impl UserDesc {
    pub fn new(id: Oid, name: String, options: UserOptions) -> Self {
        Self { id, name, options }
    }

    pub fn options(&self) -> &UserOptions {
        &self.options
    }

    pub fn rename(mut self, new_name: String) -> Self {
        self.name = new_name;
        self
    }
}

impl Eq for UserDesc {}

impl PartialEq for UserDesc {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.name == other.name
    }
}

impl Identifier<Oid> for UserDesc {
    fn id(&self) -> &Oid {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Default, Clone, Builder)]
#[builder(setter(into, strip_option), default)]
pub struct UserOptions {
    password: Option<String>,
    must_change_password: Option<bool>,
    rsa_public_key: Option<String>,
    comment: Option<String>,
}

impl UserOptions {
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }
    pub fn must_change_password(&self) -> Option<bool> {
        self.must_change_password
    }
    pub fn rsa_public_key(&self) -> Option<&str> {
        self.rsa_public_key.as_deref()
    }
    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }
}

impl Display for UserOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref e) = self.must_change_password {
            write!(f, "must_change_password={},", e)?;
        }

        if let Some(ref e) = self.comment {
            write!(f, "comment={},", e)?;
        }

        Ok(())
    }
}

pub enum AuthType<'a> {
    Password(Option<&'a str>),
    Rsa(&'a str),
}

impl<'a> From<&'a UserOptions> for AuthType<'a> {
    fn from(options: &'a UserOptions) -> Self {
        if let Some(key) = options.rsa_public_key() {
            return Self::Rsa(key);
        }

        Self::Password(options.password())
    }
}

impl<'a> AuthType<'a> {
    pub fn access_check(&self, user_info: &UserInfo) -> Result<()> {
        let user_name = user_info.user.as_str();
        let password = user_info.password.as_str();

        match self {
            Self::Password(e) => {
                let _password = e.ok_or_else(|| AuthError::PasswordNotSet)?;
                // TODO Do not enable password authentication for now
                // if password != user_info.password {
                //     return Err(AuthError::AccessDenied {
                //         user_name: user_name.to_string(),
                //         auth_type: "password".to_string(),
                //         err: "".to_string(),
                //     });
                // }

                Ok(())
            }
            Self::Rsa(public_key_pem) => {
                let private_key_pem =
                    user_info
                        .private_key
                        .as_ref()
                        .ok_or_else(|| AuthError::AccessDenied {
                            user_name: user_name.to_string(),
                            auth_type: "RSA".to_string(),
                            err: "client no private key".to_string(),
                        })?;

                let success = rsa_utils::verify(
                    private_key_pem.as_bytes(),
                    password,
                    public_key_pem.as_bytes(),
                )?;

                if !success {
                    return Err(AuthError::AccessDenied {
                        user_name: user_name.to_string(),
                        auth_type: "RSA".to_string(),
                        err: "invalid certificate".to_string(),
                    });
                }

                Ok(())
            }
        }
    }
}

#[derive(Clone)]
pub struct UserInfo {
    pub user: String,
    pub password: String,
    pub private_key: Option<String>,
}