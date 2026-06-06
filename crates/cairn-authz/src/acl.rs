//! Canned-ACL expansion (ARCH §15.7) and the mapping from ACL [`Permission`] to the
//! [`Action`]s it satisfies.

use cairn_types::{Acl, Action, Grant, Grantee, Permission, Resource, UserId};

/// The well-known log-delivery group is modelled as the [`Grantee::LogDelivery`] group.
///
/// Expand a canned-ACL name into a concrete [`Acl`] owned by `owner`. Returns `None` for an
/// unrecognised name. The supported names follow ARCH §15.7:
/// `private`, `public-read`, `public-read-write`, `authenticated-read`, `bucket-owner-read`,
/// `bucket-owner-full-control`, and `log-delivery-write`.
///
/// `bucket-owner-read` / `bucket-owner-full-control` are object-canned ACLs that additionally
/// grant the bucket owner; here `owner` is the object owner and we only have the one owner id
/// to work with, so those grants are expressed against `owner` (the bucket owner being the
/// object owner in the enforced/preferred modes Cairn steers toward). The full-control grant
/// to the owner is always present.
#[must_use]
pub fn expand_canned_acl(name: &str, owner: &UserId) -> Option<Acl> {
    let owner_full = Grant {
        grantee: Grantee::User(owner.clone()),
        permission: Permission::FullControl,
    };
    let grants = match name {
        "private" => vec![owner_full],
        "public-read" => vec![
            owner_full,
            Grant {
                grantee: Grantee::AllUsers,
                permission: Permission::Read,
            },
        ],
        "public-read-write" => vec![
            owner_full,
            Grant {
                grantee: Grantee::AllUsers,
                permission: Permission::Read,
            },
            Grant {
                grantee: Grantee::AllUsers,
                permission: Permission::Write,
            },
        ],
        "authenticated-read" => vec![
            owner_full,
            Grant {
                grantee: Grantee::AuthenticatedUsers,
                permission: Permission::Read,
            },
        ],
        // `bucket-owner-read` / `bucket-owner-full-control`: the owner already holds full
        // control; the additional bucket-owner grant collapses onto the same id here.
        "bucket-owner-read" | "bucket-owner-full-control" => vec![owner_full],
        "log-delivery-write" => vec![
            owner_full,
            Grant {
                grantee: Grantee::LogDelivery,
                permission: Permission::Write,
            },
            Grant {
                grantee: Grantee::LogDelivery,
                permission: Permission::ReadAcp,
            },
        ],
        _ => return None,
    };
    Some(Acl {
        owner: owner.clone(),
        grants,
    })
}

/// Whether an ACL `permission` granted on a resource of the same kind as `resource` satisfies
/// `action`. Implements the mapping of ARCH §15.7:
///
/// * `Read` on an object => `GetObject*` (data, version, attributes, tagging-get reads).
/// * `Read` on a bucket => `ListBucket*`.
/// * `Write` on a bucket => `PutObject` / `DeleteObject*` and object subresource writes.
/// * `ReadAcp` => `Get*Acl`.
/// * `WriteAcp` => `Put*Acl`.
/// * `FullControl` => everything any of the above grant.
#[must_use]
pub fn permission_satisfies(permission: Permission, action: Action, resource: &Resource) -> bool {
    let on_object = matches!(resource, Resource::Object { .. });
    match permission {
        Permission::FullControl => true,
        Permission::ReadAcp => is_read_acp(action),
        Permission::WriteAcp => is_write_acp(action),
        Permission::Read => {
            if on_object {
                is_object_read(action)
            } else {
                is_bucket_read(action)
            }
        }
        Permission::Write => {
            // Write is a bucket-level permission that governs creating/overwriting/deleting
            // the bucket's objects; on an object resource it also covers object-data writes.
            is_object_write(action)
        }
    }
}

fn is_read_acp(action: Action) -> bool {
    matches!(action, Action::GetObjectAcl | Action::GetBucketAcl)
}

fn is_write_acp(action: Action) -> bool {
    matches!(action, Action::PutObjectAcl | Action::PutBucketAcl)
}

/// Object-data and object-read subresource actions a `Read` grant on an object satisfies.
fn is_object_read(action: Action) -> bool {
    matches!(
        action,
        Action::GetObject
            | Action::GetObjectVersion
            | Action::GetObjectTagging
            | Action::GetObjectAttributes
            | Action::ListMultipartUploadParts
    )
}

/// Bucket-listing actions a `Read` grant on a bucket satisfies.
fn is_bucket_read(action: Action) -> bool {
    matches!(
        action,
        Action::ListBucket
            | Action::ListBucketVersions
            | Action::ListBucketMultipartUploads
            | Action::GetBucketLocation
    )
}

/// Object-write actions a `Write` grant satisfies (bucket `Write` governs object lifecycle).
fn is_object_write(action: Action) -> bool {
    matches!(
        action,
        Action::PutObject
            | Action::DeleteObject
            | Action::DeleteObjectVersion
            | Action::PutObjectTagging
            | Action::DeleteObjectTagging
            | Action::AbortMultipartUpload
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cairn_types::{BucketName, ObjectKey};

    fn uid(s: &str) -> UserId {
        UserId(s.to_owned())
    }
    fn obj() -> Resource {
        Resource::Object {
            bucket: BucketName::parse("b-name").unwrap(),
            key: ObjectKey::parse("k").unwrap(),
        }
    }
    fn buck() -> Resource {
        Resource::Bucket(BucketName::parse("b-name").unwrap())
    }

    #[test]
    fn unknown_canned_acl_is_none() {
        assert!(expand_canned_acl("nope", &uid("o")).is_none());
    }

    #[test]
    fn private_is_owner_full_control() {
        let acl = expand_canned_acl("private", &uid("o")).unwrap();
        assert_eq!(acl.grants.len(), 1);
        assert_eq!(acl.grants[0].grantee, Grantee::User(uid("o")));
        assert_eq!(acl.grants[0].permission, Permission::FullControl);
    }

    #[test]
    fn public_read_adds_all_users_read() {
        let acl = expand_canned_acl("public-read", &uid("o")).unwrap();
        assert!(
            acl.grants
                .iter()
                .any(|g| g.grantee == Grantee::AllUsers && g.permission == Permission::Read)
        );
        assert!(
            !acl.grants
                .iter()
                .any(|g| g.grantee == Grantee::AllUsers && g.permission == Permission::Write)
        );
    }

    #[test]
    fn public_read_write_adds_write() {
        let acl = expand_canned_acl("public-read-write", &uid("o")).unwrap();
        assert!(
            acl.grants
                .iter()
                .any(|g| g.grantee == Grantee::AllUsers && g.permission == Permission::Write)
        );
    }

    #[test]
    fn authenticated_read_uses_auth_group() {
        let acl = expand_canned_acl("authenticated-read", &uid("o")).unwrap();
        assert!(
            acl.grants
                .iter()
                .any(|g| g.grantee == Grantee::AuthenticatedUsers
                    && g.permission == Permission::Read)
        );
    }

    #[test]
    fn log_delivery_write() {
        let acl = expand_canned_acl("log-delivery-write", &uid("o")).unwrap();
        assert!(
            acl.grants
                .iter()
                .any(|g| g.grantee == Grantee::LogDelivery && g.permission == Permission::Write)
        );
    }

    #[test]
    fn permission_mapping() {
        // Read on object -> GetObject, not PutObject.
        assert!(permission_satisfies(
            Permission::Read,
            Action::GetObject,
            &obj()
        ));
        assert!(!permission_satisfies(
            Permission::Read,
            Action::PutObject,
            &obj()
        ));
        // Read on bucket -> ListBucket.
        assert!(permission_satisfies(
            Permission::Read,
            Action::ListBucket,
            &buck()
        ));
        assert!(!permission_satisfies(
            Permission::Read,
            Action::GetObject,
            &buck()
        ));
        // Write -> PutObject / DeleteObject.
        assert!(permission_satisfies(
            Permission::Write,
            Action::PutObject,
            &obj()
        ));
        assert!(permission_satisfies(
            Permission::Write,
            Action::DeleteObject,
            &obj()
        ));
        // ReadAcp / WriteAcp.
        assert!(permission_satisfies(
            Permission::ReadAcp,
            Action::GetObjectAcl,
            &obj()
        ));
        assert!(permission_satisfies(
            Permission::WriteAcp,
            Action::PutObjectAcl,
            &obj()
        ));
        assert!(!permission_satisfies(
            Permission::ReadAcp,
            Action::GetObject,
            &obj()
        ));
        // FullControl -> everything.
        assert!(permission_satisfies(
            Permission::FullControl,
            Action::DeleteObjectVersion,
            &obj()
        ));
    }
}
