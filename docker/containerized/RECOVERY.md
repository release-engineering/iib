# Recovering an interrupted index overwrite

This procedure applies to a catalog with one sequential SAC writer. Wait for any earlier
registry tag write to settle before starting another request. The worker selects the
database and Git catalog from the digest currently held by the public tag.

## Rollout for existing digests

Existing `idb-D` artifacts may lack the Git binding annotations. The worker stops an
overwrite on such a digest until the artifact has a verified `idb-D-binding` sidecar.
The sidecar leaves the original artifact unchanged.

For each public tag:

1. Resolve the tag to a digest and record the full resolved pullspec. Resolve its
   `idb-D` artifact to a manifest digest. Pull that exact manifest and record the
   SHA256 of its `index.db` payload.
2. Independently establish the Git commit used for that published image. Compare
   the image's catalog with the commit's complete `configs/` tree, including deleted
   files and symlinks. Confirm the producer in `.iib-build-metadata.json` and the
   database contents. Do not use the current branch tip as evidence by itself.
3. If the public image shows newer catalog content than the database represents,
   first publish a corrected successor image and database preserving the visible
   catalog. Verify that new pair before continuing. Do not bind an inconsistent pair.
4. In a worker environment with Git and ORAS credentials, call
   `iib.workers.tasks.oras_utils.publish_verified_legacy_binding()` with the
   digest-pinned image, a temporary directory, the verified Git checkout path,
   commit, `configs/` tree ID, producer request ID, database SHA256, and original
   artifact manifest digest. The function checks these inputs, retains the commit
   under `refs/tags/iib-index-D`, and writes the separate sidecar. A repeat with
   the same inputs is safe; a conflicting binding fails.
5. Call `read_published_index_db_artifact()` for the same resolved image and verify
   its returned manifest, commit, tree, producer and database SHA256. Confirm the
   public tag still resolves to the recorded digest.

The operator must establish the image-to-Git relationship in step 2. The onboarding
function can verify the supplied Git revision and database bytes, but cannot infer
which revision Konflux used to build a legacy image.

Keep the retained Git tags and the digest-keyed Quay artifact tags immutable. The
worker checks an existing binding before publishing and reads it back afterward;
registry tag immutability protects against writers outside the sequential contract.

## Acceptance sequence

In the containerized development environment, exercise both states with an FBC-only
request B followed by add request C:

- Public D0, with A at 1.0 in both image and SQLite, while Git contains an
  unpublished 1.1 change. Confirm B's repair commit restores the D0 catalog before
  B applies its operation; inspect the B and C image catalogs and SQLite databases.
- Public D1, with A at 1.1 in the catalog and A absent from SQLite. Confirm B starts
  from D1 and C does not render A from an obsolete database row.

Also exercise a crash after the repair commit, MR source branch cleanup, an existing
conflicting binding, and a failed B build after a successful repair. The repaired
branch must stay at the published catalog when B fails.
