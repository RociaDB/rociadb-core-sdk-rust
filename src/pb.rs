//! Generated protobuf types and gRPC clients.
//!
//! This module is internal to the crate. It is regenerated from
//! `proto/upstream/v1/upstream.proto` by prost/tonic on every build, and a
//! routine prost or tonic upgrade can change field types, add fields, or
//! otherwise reshape these generated types without the SDK's own API
//! changing at all — which is why nothing outside the crate can name a type
//! here, and why the module is not part of the semver contract.
//!
//! The handful of generated types that genuinely are part of the public
//! contract — because they appear in a public method signature and callers
//! need to name them — are re-exported individually at the crate root
//! instead: [`crate::CollectionInfo`], [`crate::Neighbor`],
//! [`crate::UploadRequest`], and [`crate::DownloadResponse`]. Depend on those
//! re-exports, not on paths reaching into `pb` directly. `StatResponse` used to
//! be among them and no longer is: [`crate::RociaDbClient::stat_file`] converts
//! it into the SDK's own [`crate::FileMetadata`] before returning, so nothing
//! generated reaches that signature.
//!
//! The crate-wide `missing_docs` lint is switched off for everything below:
//! the generated code documents only what the build script attaches to it (the
//! four re-exported types and their fields), and the rest is internal.
#![allow(missing_docs)]

pub mod upstream {
    /// Generated code for the rocia.v1 API.
    pub mod v1 {
        tonic::include_proto!("rocia.v1");
    }
}
