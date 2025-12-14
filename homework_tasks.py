from postgre_utils import db_connection, execute_query
import time

def run_query_with_timing(query: str, description: str):
    """Run a query and measure execution time"""
    print(f"\n{description}")
    print(f"Query: {query[:80]}{'...' if len(query) > 80 else ''}")

    start_time = time.time()
    try:
        results = execute_query(query)
        end_time = time.time()
        execution_time = end_time - start_time

        print(f"Execution time: {execution_time:.4f} seconds")
        print(f"Rows returned: {len(results)}")

        return results, execution_time
    except Exception as e:
        print(f"Error: {e}")
        return [], 0


def run_explain_analyze(query: str, description: str):
    """Run EXPLAIN ANALYZE on query"""
    print(f"\n{description}")
    explain_query = f"EXPLAIN ANALYZE {query}"

    try:
        results = execute_query(explain_query)
        print("Execution Plan:")
        for row in results:
            print(f"   {row['QUERY PLAN']}")
    except Exception as e:
        print(f"Error running EXPLAIN ANALYZE: {e}")


def task1_spatial_search():
    """TASK 1: Spatial Search with and without Index"""
    print("\n" + "="*80)
    print("TASK 1: Spatial Search with and without Index")
    print("="*80)

    # Drop indexes if they exist
    print("Dropping indexes if they exist...")
    drop_index_queries = [
        "DROP INDEX IF EXISTS idx_cafes_geom_gist;"
    ]
    for q in drop_index_queries:
        execute_query(q, fetch=False)

    # Step 1: Search cafes in rectangle WITHOUT index
    print("\nStep 1: Search cafes in rectangle WITHOUT index")
    query1 = """
    SELECT id, name,
           ST_X(geom) as longitude,
           ST_Y(geom) as latitude
    FROM cafes
    WHERE ST_Within(geom, ST_MakeEnvelope(37.5, 55.6, 37.7, 55.8, 4326));
    """
    results1, time1 = run_query_with_timing(query1, "Search cafes in rectangle WITHOUT index")
    run_explain_analyze(query1, "EXPLAIN ANALYZE for search WITHOUT index")

    # Step 2: Create GiST index
    print("\nStep 2: Create GiST index on cafes table")
    create_index_query = "CREATE INDEX idx_cafes_geom_gist ON cafes USING GIST(geom);"
    execute_query(create_index_query, fetch=False)
    print("Created GiST index on geom column")

    # Step 3: Search cafes in rectangle WITH index
    print("\nStep 3: Search cafes in rectangle WITH index")
    results2, time2 = run_query_with_timing(query1, "Search cafes in rectangle WITH index")
    run_explain_analyze(query1, "EXPLAIN ANALYZE for search WITH index")

    # Step 4: Compare performance
    print("\nStep 4: Performance comparison")
    print(f"    Time WITHOUT index: {time1:.4f} seconds")
    print(f"    Time WITH index: {time2:.4f} seconds")
    if time1 > 0 and time2 > 0:
        speedup = time1 / time2
        print(f"    Speedup: {speedup:.2f}x faster with index")

    return time1, time2


def task2_knn_search():
    """TASK 2: KNN Search for nearest objects"""
    print("\n" + "="*80)
    print("TASK 2: KNN Search for nearest objects")
    print("="*80)

    # Check if GiST index exists
    print("\nChecking for GiST index...")
    check_index = """
    SELECT indexname FROM pg_indexes
    WHERE tablename = 'cafes' AND indexdef LIKE '%geom%';
    """
    indexes = execute_query(check_index)
    if indexes:
        print("Found indexes:")
        for idx in indexes:
            print(f"    {idx['indexname']}")
    else:
        print("No spatial index found. Creating one...")
        execute_query("CREATE INDEX IF NOT EXISTS idx_cafes_geom_gist ON cafes USING GIST(geom);", fetch=False)

    # Step 1: KNN search for 3 nearest cafes
    print("\nStep 1: Find 3 nearest cafes to point (37.617, 55.751)")
    query = """
    SELECT id, name,
        ST_X(geom) as longitude,
        ST_Y(geom) as latitude,
        ST_Distance(geom, ST_SetSRID(ST_MakePoint(37.617, 55.751), 4326)) as distance
    FROM cafes
    ORDER BY geom <-> ST_SetSRID(ST_MakePoint(37.617, 55.751), 4326)
    LIMIT 3;
    """
    results, time_taken = run_query_with_timing(query, "KNN search for 3 nearest cafes")

    # Display results
    print("Nearest cafes:")
    for i, cafe in enumerate(results, 1):
        print(f"   {i}. {cafe['name']} - Distance: {cafe['distance']:.6f}")

    # Step 2: EXPLAIN ANALYZE
    print("\nStep 2: EXPLAIN ANALYZE for KNN search")
    run_explain_analyze(query, "EXPLAIN ANALYZE for KNN search")

    return results


def task3_partial_indexes():
    """TASK 3: Partial Indexes"""
    print("\n" + "="*80)
    print("TASK 3: Partial Indexes")
    print("="*80)

    # Drop existing indexes
    execute_query("DROP INDEX IF EXISTS idx_buildings_geom_full;", fetch=False)
    execute_query("DROP INDEX IF EXISTS idx_buildings_geom_historic;", fetch=False)

    # Step 1: Create full spatial index
    print("\nStep 1: Create full spatial index on buildings")
    create_full_index = """
    CREATE INDEX idx_buildings_geom_full ON buildings USING GIST(geom);
    """
    execute_query(create_full_index, fetch=False)
    print("Created full spatial index on buildings")

    # Step 2: Create partial spatial index for historic buildings
    print("\nStep 2: Create partial spatial index for historic buildings")
    create_partial_index = """
    CREATE INDEX idx_buildings_geom_historic ON buildings USING GIST(geom)
    WHERE is_historic = true;
    """
    execute_query(create_partial_index, fetch=False)
    print("Created partial spatial index for historic buildings")

    # Step 3: Compare index sizes
    print("\nStep 3: Compare index sizes")
    size_query = """
    SELECT
        'full_index' as index_type,
        pg_size_pretty(pg_relation_size('idx_buildings_geom_full')) as index_size,
        pg_relation_size('idx_buildings_geom_full') as size_bytes
    UNION ALL
    SELECT
        'partial_index' as index_type,
        pg_size_pretty(pg_relation_size('idx_buildings_geom_historic')) as index_size,
        pg_relation_size('idx_buildings_geom_historic') as size_bytes
    ORDER BY size_bytes DESC;
    """
    size_results = execute_query(size_query)
    print("Index size comparison:")
    for row in size_results:
        print(f"   {row['index_type']}: {row['index_size']}")

    # Step 4: Find historic buildings in center
    print("\nStep 4: Find historic buildings in center area")
    search_query = """
    SELECT id, name, is_historic
    FROM buildings
    WHERE is_historic = true
      AND ST_Within(geom, ST_MakeEnvelope(37.5, 55.6, 37.7, 55.8, 4326));
    """
    results, time_taken = run_query_with_timing(search_query, "Find historic buildings in center area")
    print(f"Found {len(results)} historic buildings in center area")

    # Show EXPLAIN ANALYZE
    run_explain_analyze(search_query, "EXPLAIN ANALYZE for historic buildings search")

    return size_results, results


def main():
    """Run all homework tasks in order"""
    print("Spatial Indexing Homework Tasks")
    print("="*80)
    print("Running queries exactly as specified in homework.pdf")

    try:
        # Connect to database
        db_connection.connect()
        print("Connected to database")

        # Check if tables exist
        try:
            cafes_count = execute_query("SELECT COUNT(*) as count FROM cafes;")
            print(f"Found {cafes_count[0]['count']} cafes in database")
        except Exception as e:
            print(f"Cafes table not found: {e}")
            print("Please run 'python postgre_utils.py' first to create tables")
            return

        # TASK 1: Spatial Search with and without Index
        time_without, time_with = task1_spatial_search()

        # TASK 2: KNN Search
        knn_results = task2_knn_search()

        # TASK 3: Partial Indexes
        size_results, historic_results = task3_partial_indexes()

        # Summary
        print("\n" + "="*80)
        print("ALL HOMEWORK TASKS COMPLETED!")
        print("="*80)

        print("\nSUMMARY:")
        print(f"    Task 1 - Spatial Search: {time_without:.4f}s → {time_with:.4f}s")
        if time_without > 0 and time_with > 0:
            speedup = time_without / time_with
            print(f"     Speedup: {speedup:.2f}x faster with index")
        print(f"    Task 2 - KNN Search: Found {len(knn_results)} nearest cafes")
        print(f"    Task 3 - Partial Indexes: Found {len(historic_results)} historic buildings")

        if size_results:
            print("    Index Sizes:")
            for row in size_results:
                print(f"     - {row['index_type']}: {row['index_size']}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db_connection.disconnect()
        print("Disconnected from database")


if __name__ == "__main__":
    main()