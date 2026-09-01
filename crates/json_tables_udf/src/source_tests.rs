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

/// BUG-135: a missing table or column used to surface as a protocol error from
/// the loader's own SELECT ("object DOC not found"), which points at the wrong
/// layer. These cover the catalog pre-check that replaces it.
mod table_source_diagnostics {
    use super::*;
    use exasol_udf_sdk::connect_back::ExaConnection;
    use exasol_udf_sdk::value::Value;

    /// Answers the catalog query with a fixed column list.
    struct Catalog {
        rows: Vec<(&'static str, &'static str, &'static str)>,
    }

    impl ExaConnection for Catalog {
        fn query_for_each(
            &mut self,
            sql: &str,
            f: &mut dyn FnMut(Vec<Value>) -> Result<(), UdfError>,
        ) -> Result<(), UdfError> {
            assert!(sql.contains("EXA_ALL_COLUMNS"), "unexpected query: {sql}");
            for (table, name, ty) in &self.rows {
                f(vec![
                    Value::String((*table).to_string()),
                    Value::String((*name).to_string()),
                    Value::String((*ty).to_string()),
                ])?;
            }
            Ok(())
        }

        fn execute(&mut self, _sql: &str) -> Result<u64, UdfError> {
            Ok(0)
        }
    }

    fn check(
        rows: Vec<(&'static str, &'static str, &'static str)>,
        table: &str,
        column: &str,
    ) -> Result<(), String> {
        let mut connection: Box<dyn ExaConnection> = Box::new(Catalog { rows });
        check_table_source(&mut connection, "SRC", table, column).map_err(|err| format!("{err:?}"))
    }

    const ORDERS: [(&str, &str, &str); 3] = [
        ("orders", "ID", "DECIMAL(18,0)"),
        ("orders", "PAYLOAD", "VARCHAR(2000000) UTF8"),
        ("orders", "CREATED_AT", "TIMESTAMP"),
    ];

    #[test]
    fn an_existing_text_column_passes() {
        assert!(check(ORDERS.to_vec(), "orders", "PAYLOAD").is_ok());
    }

    #[test]
    fn the_default_doc_column_names_the_source_and_the_option() {
        let err = check(ORDERS.to_vec(), "orders", "DOC").unwrap_err();
        assert!(err.contains("SRC.orders has no column DOC"), "{err}");
        assert!(err.contains("table://SRC.orders.COLUMN"), "{err}");
        // The columns that do exist are the thing the caller needs next.
        assert!(err.contains("ID, PAYLOAD, CREATED_AT"), "{err}");
    }

    #[test]
    fn a_missing_table_is_reported_as_a_missing_table() {
        let err = check(vec![], "nope", "DOC").unwrap_err();
        assert!(err.contains("SRC.nope does not exist"), "{err}");
        assert!(err.contains("CONNECTION"), "{err}");
    }

    #[test]
    fn a_case_mismatch_on_the_table_says_so() {
        // Ingest creates lower-case table names, so this is a likely mistake.
        let err = check(ORDERS.to_vec(), "ORDERS", "PAYLOAD").unwrap_err();
        assert!(
            err.contains("SRC.ORDERS does not exist, but SRC.orders does"),
            "{err}"
        );
        assert!(err.contains("upper case"), "{err}");
    }

    #[test]
    fn a_case_mismatch_on_the_column_suggests_the_real_one() {
        let err = check(ORDERS.to_vec(), "orders", "payload").unwrap_err();
        assert!(err.contains("did you mean PAYLOAD?"), "{err}");
    }

    #[test]
    fn a_non_text_column_explains_what_the_loader_needs() {
        let err = check(ORDERS.to_vec(), "orders", "ID").unwrap_err();
        assert!(err.contains("is DECIMAL(18,0)"), "{err}");
        assert!(err.contains("CHAR or VARCHAR"), "{err}");
    }

    #[test]
    fn a_wide_table_gets_a_summarised_column_list() {
        let mut rows: Vec<(&'static str, &'static str, &'static str)> = Vec::new();
        for name in [
            "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12",
            "C13", "C14",
        ] {
            rows.push(("wide", name, "VARCHAR(10) UTF8"));
        }
        let err = check(rows, "wide", "DOC").unwrap_err();
        assert!(err.contains("C12, … (2 more)"), "{err}");
        assert!(!err.contains("C13"), "{err}");
    }

    #[test]
    fn char_and_varchar_are_both_accepted() {
        assert!(is_text_type("VARCHAR(2000000) UTF8"));
        assert!(is_text_type("CHAR(10) ASCII"));
        assert!(!is_text_type("DECIMAL(18,0)"));
        assert!(!is_text_type("TIMESTAMP"));
    }
}
