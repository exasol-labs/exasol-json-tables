use super::*;

#[test]
fn parses_every_supported_locator() {
    assert_eq!(
        Source::parse("bfs:/buckets/bfsdefault/default/orders.json").unwrap(),
        Source::BucketFs {
            path: "/buckets/bfsdefault/default/orders.json".to_string()
        }
    );
    assert_eq!(
        Source::parse("/buckets/bfsdefault/default/orders.json").unwrap(),
        Source::BucketFs {
            path: "/buckets/bfsdefault/default/orders.json".to_string()
        }
    );
    assert_eq!(
        Source::parse("table://LANDING.DOCS").unwrap(),
        Source::Table {
            schema: "LANDING".to_string(),
            table: "DOCS".to_string(),
            column: "DOC".to_string(),
            order_by: None,
        }
    );
    assert_eq!(
        Source::parse("table://LANDING.DOCS.PAYLOAD").unwrap(),
        Source::Table {
            schema: "LANDING".to_string(),
            table: "DOCS".to_string(),
            column: "PAYLOAD".to_string(),
            order_by: None,
        }
    );
    assert_eq!(
        Source::parse("exatunnel://10.88.0.2:39769").unwrap(),
        Source::Http {
            host: "10.88.0.2".to_string(),
            port: 39769,
            path: "/".to_string(),
        }
    );
    assert_eq!(
        Source::parse("http://10.88.0.2:8080/data/orders.ndjson").unwrap(),
        Source::Http {
            host: "10.88.0.2".to_string(),
            port: 8080,
            path: "/data/orders.ndjson".to_string(),
        }
    );
}

#[test]
fn s3_splits_into_the_bucket_url_and_object_key_import_wants() {
    assert_eq!(
        Source::parse("s3://acme-lake/events/2026-08-31/part-0.ndjson").unwrap(),
        Source::Cloud {
            url: "https://acme-lake.s3.amazonaws.com".to_string(),
            file: "events/2026-08-31/part-0.ndjson".to_string(),
        }
    );
    assert_eq!(
        Source::parse("https://acme.blob.core.windows.net/lake/orders.ndjson").unwrap(),
        Source::Cloud {
            url: "https://acme.blob.core.windows.net".to_string(),
            file: "lake/orders.ndjson".to_string(),
        }
    );
}

#[test]
fn unsupported_and_malformed_locators_are_rejected() {
    for locator in [
        "",
        "   ",
        "ftp://host/file",
        "table://ONLYONE",
        "exatunnel://nohost",
    ] {
        let err = Source::parse(locator).unwrap_err();
        assert!(
            matches!(err, UdfError::User(_)),
            "{locator} produced {err:?}"
        );
    }
}

#[test]
fn rereadability_decides_whether_a_landing_table_is_needed() {
    assert!(Source::parse("bfs:/buckets/x/y.json")
        .unwrap()
        .is_rereadable());
    assert!(Source::parse("table://S.T").unwrap().is_rereadable());
    // A tunnel serves its bytes once; a cloud object needs the engine to fetch it.
    assert!(!Source::parse("exatunnel://host:1").unwrap().is_rereadable());
    assert!(!Source::parse("s3://b/k.ndjson").unwrap().is_rereadable());
}

#[test]
fn provenance_records_the_source_kind() {
    assert_eq!(
        Source::parse("bfs:/buckets/x/y.json")
            .unwrap()
            .connection_kind(),
        "bucketfs"
    );
    assert_eq!(
        Source::parse("s3://b/k").unwrap().connection_kind(),
        "cloud-storage"
    );
    assert_eq!(
        Source::parse("exatunnel://h:1").unwrap().connection_kind(),
        "http-stream"
    );
    assert_eq!(
        Source::parse("table://S.T").unwrap().connection_kind(),
        "table"
    );
}

#[test]
fn chunking_never_splits_a_character() {
    // Multi-byte characters must not be cut in half when a stream is chunked
    // into VARCHAR rows.
    let text = "aä€😀b";
    for max in 0..=text.chars().count() + 2 {
        let boundary = char_boundary(text, max);
        assert!(text.is_char_boundary(boundary), "max={max}");
    }
    assert_eq!(char_boundary(text, 2), 3); // 'a' + 'ä'(2 bytes)
    assert_eq!(char_boundary(text, 99), text.len());
}

#[test]
fn a_bucketfs_source_reads_a_real_file() {
    let dir = std::env::temp_dir().join(format!("jt_udf_src_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("docs.ndjson");
    std::fs::write(&path, "{\"a\":1}\n").unwrap();

    let source = Source::BucketFs {
        path: path.to_string_lossy().to_string(),
    };
    assert_eq!(read_to_string(&source, None).unwrap(), "{\"a\":1}\n");

    let missing = Source::BucketFs {
        path: dir.join("nope.json").to_string_lossy().to_string(),
    };
    let err = read_to_string(&missing, None).unwrap_err();
    assert!(format!("{err:?}").contains("cannot read"), "{err:?}");
    std::fs::remove_dir_all(&dir).ok();
}
