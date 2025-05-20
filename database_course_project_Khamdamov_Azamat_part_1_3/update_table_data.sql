CREATE OR REPLACE FUNCTION update_table_data(
    table_name TEXT,
    primary_key_column TEXT,
    primary_key_value TEXT,
    column_to_update TEXT,
    new_value TEXT
)
RETURNS TEXT AS $
DECLARE
    update_query TEXT;
    affected_rows INTEGER;
BEGIN
    -- Input validation
    IF table_name IS NULL OR primary_key_column IS NULL OR 
       primary_key_value IS NULL OR column_to_update IS NULL THEN
        RETURN 'Error: Missing required parameters';
    END IF;
    
    -- Check if table exists (PostgreSQL specific schema search)
    IF NOT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = update_table_data.table_name
        AND table_schema = 'public'
    ) THEN
        RETURN 'Error: Table ' || table_name || ' does not exist';
    END IF;
    
    -- Check if columns exist in the table (PostgreSQL specific schema search)
    IF NOT EXISTS (
        SELECT FROM information_schema.columns 
        WHERE table_name = update_table_data.table_name 
        AND column_name = update_table_data.primary_key_column
        AND table_schema = 'public'
    ) THEN
        RETURN 'Error: Primary key column ' || primary_key_column || ' does not exist in table ' || table_name;
    END IF;
    
    IF NOT EXISTS (
        SELECT FROM information_schema.columns 
        WHERE table_name = update_table_data.table_name 
        AND column_name = update_table_data.column_to_update
        AND table_schema = 'public'
    ) THEN
        RETURN 'Error: Column ' || column_to_update || ' does not exist in table ' || table_name;
    END IF;
    
    -- Construct and execute the update query with proper PostgreSQL quoting to prevent SQL injection
    update_query := 'UPDATE ' || quote_ident(table_name) || 
                   ' SET ' || quote_ident(column_to_update) || ' = $1' ||
                   ' WHERE ' || quote_ident(primary_key_column) || ' = $2';
    
    EXECUTE update_query USING new_value, primary_key_value;
    
    -- Check how many rows were updated (PostgreSQL specific diagnostic)
    GET DIAGNOSTICS affected_rows = ROW_COUNT;
    
    IF affected_rows = 0 THEN
        RETURN 'Warning: No rows were updated. The specified primary key value might not exist.';
    ELSIF affected_rows = 1 THEN
        RETURN 'Success: Updated column ' || column_to_update || ' for row with ' || 
               primary_key_column || ' = ' || primary_key_value;
    ELSE
        RETURN 'Warning: Multiple rows (' || affected_rows || ') were updated. ' ||
               'This suggests the column specified is not a proper primary key.';
    END IF;
    
EXCEPTION
    WHEN others THEN
        RETURN 'Error: ' || SQLERRM;
END;
$ LANGUAGE plpgsql;
