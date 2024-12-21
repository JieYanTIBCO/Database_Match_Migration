CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    customer_id TEXT PRIMARY KEY,                -- PK, hashed varchar
    name TEXT NOT NULL,                          -- Customer name
    gender TEXT DEFAULT 'O' CHECK (gender IN ('M', 'F', 'O')), -- Gender (M, F, or O for Other)
    DOB DATE NOT NULL,                           -- Date of Birth
    Nationality TEXT NOT NULL                    -- Nationality
);
