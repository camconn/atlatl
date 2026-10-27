//! Secret references (spec 02 §2.2). Every resolved value is registered with the redactor.

use std::path::Path;

use crate::config::types::{SecretRef, SecretValue};
use crate::redact::Redactor;
use crate::template::EnvSource;

pub mod aws;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SecretError(pub String);

pub fn resolve_ref(
    r: &SecretRef,
    env: &dyn EnvSource,
    project_root: &Path,
) -> Result<String, SecretError> {
    match r {
        SecretRef::Env(name) => env.var(name).ok_or_else(|| {
            SecretError(format!(
                "secret `env:{name}`: environment variable is not set"
            ))
        }),
        SecretRef::File(p) => {
            let path = project_root.join(p);
            std::fs::read_to_string(&path)
                .map(|s| s.trim_end_matches(['\n', '\r']).to_owned())
                .map_err(|e| SecretError(format!("secret `file:{p}`: {e}")))
        }
        SecretRef::AwsSecretsManager { arn, json_key } => {
            aws::resolve(arn, json_key.as_deref(), env)
        }
    }
}

/// Resolves a secret in place to a literal and registers it. `rendered` literals (templates) are
/// registered too; plain literals are left alone (they are linted, not hidden).
pub fn resolve_in_place(
    v: &mut SecretValue,
    was_template: bool,
    env: &dyn EnvSource,
    project_root: &Path,
    redactor: &Redactor,
) -> Result<(), SecretError> {
    match v {
        SecretValue::Ref(r) => {
            let s = resolve_ref(r, env, project_root)?;
            redactor.register(&s);
            *v = SecretValue::Literal(s);
        }
        SecretValue::Literal(s) => {
            if was_template {
                redactor.register(s);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template::MapEnv;

    #[test]
    fn env_and_file_refs() {
        let env = MapEnv::new([("PW", "hunter2")]);
        let dir = tempfile::tempdir().expect("tmp");
        std::fs::write(dir.path().join("tok"), "t0k3n\n").expect("write");
        let r = Redactor::new();

        let mut v = SecretValue::Ref(SecretRef::Env("PW".into()));
        resolve_in_place(&mut v, false, &env, dir.path(), &r).expect("env");
        assert_eq!(v, SecretValue::Literal("hunter2".into()));
        assert_eq!(r.redact("pw=hunter2"), "pw=****");

        let mut v = SecretValue::Ref(SecretRef::File("tok".into()));
        resolve_in_place(&mut v, false, &env, dir.path(), &r).expect("file");
        assert_eq!(v, SecretValue::Literal("t0k3n".into()));
        assert_eq!(r.redact("t0k3n"), "****");
    }

    #[test]
    fn missing_env_and_aws_region() {
        let env = MapEnv::default();
        let e =
            resolve_ref(&SecretRef::Env("NOPE".into()), &env, Path::new(".")).expect_err("missing");
        assert!(e.0.contains("NOPE"));
        let e = resolve_ref(
            &SecretRef::AwsSecretsManager {
                arn: "db/prod".into(),
                json_key: None,
            },
            &env,
            Path::new("."),
        )
        .expect_err("aws");
        assert!(e.0.contains("no region"), "{e}");
    }

    #[test]
    fn templated_literals_are_registered() {
        let r = Redactor::new();
        let mut v = SecretValue::Literal("from-template".into());
        resolve_in_place(&mut v, true, &MapEnv::default(), Path::new("."), &r).expect("ok");
        assert_eq!(r.redact("from-template"), "****");
        let mut v = SecretValue::Literal("plain".into());
        resolve_in_place(&mut v, false, &MapEnv::default(), Path::new("."), &r).expect("ok");
        assert_eq!(r.redact("plain"), "plain");
    }
}
