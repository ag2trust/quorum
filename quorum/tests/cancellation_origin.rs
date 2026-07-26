#[test]
fn automatic_paths_cannot_originate_cancelled() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap();
    let serve = std::fs::read_to_string(root.join("quorum/src/serve/mod.rs")).unwrap();
    let sweep = std::fs::read_to_string(root.join("quorum-core/src/sweep.rs")).unwrap();
    let serve_production = serve.split("#[cfg(test)]").next().unwrap();
    let sweep_production = sweep.split("#[cfg(test)]").next().unwrap();

    assert!(
        !serve_production.contains("Event::Cancelled"),
        "daemon production code must never emit Cancelled"
    );
    assert!(
        !serve_production.contains("status: Some(\"cancelled\")"),
        "daemon production code must never write cancelled"
    );
    assert!(
        !sweep_production.contains("status='cancelled'"),
        "sweep production code must never write cancelled"
    );
}
