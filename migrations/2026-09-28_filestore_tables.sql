-- Content-addressed file store: every unique file is stored once and its text
-- extracted once, however many documents link to it.

IF OBJECT_ID('dbo.fs_stored_file') IS NULL
CREATE TABLE dbo.fs_stored_file (
    sha256        CHAR(64)        NOT NULL PRIMARY KEY,   -- fingerprint of the file bytes
    size_bytes    BIGINT          NOT NULL,
    content_type  NVARCHAR(200)   NULL,
    extension     NVARCHAR(20)    NULL,
    storage_path  NVARCHAR(500)   NOT NULL,               -- relative to FILESTORE_ROOT
    first_url     NVARCHAR(2000)  NULL,
    created_at    DATETIME2       NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('dbo.fs_url_seen') IS NULL
CREATE TABLE dbo.fs_url_seen (
    url_hash        CHAR(64)       NOT NULL PRIMARY KEY,  -- sha256 of the normalized url
    url             NVARCHAR(2000) NOT NULL,
    sha256          CHAR(64)       NULL REFERENCES dbo.fs_stored_file(sha256),
    etag            NVARCHAR(500)  NULL,
    last_modified   NVARCHAR(100)  NULL,
    first_seen_at   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    last_checked_at DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    last_changed_at DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF OBJECT_ID('dbo.fs_document_file') IS NULL
CREATE TABLE dbo.fs_document_file (
    id            BIGINT IDENTITY PRIMARY KEY,
    document_key  NVARCHAR(400)  NOT NULL,                -- article/law id or doc_path
    url_hash      CHAR(64)       NOT NULL,
    url           NVARCHAR(2000) NOT NULL,
    sha256        CHAR(64)       NOT NULL REFERENCES dbo.fs_stored_file(sha256),
    role          NVARCHAR(50)   NOT NULL DEFAULT 'attachment',
    created_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_fs_document_file UNIQUE (document_key, url_hash)
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fs_document_file_sha256')
CREATE INDEX IX_fs_document_file_sha256 ON dbo.fs_document_file(sha256);
GO

IF OBJECT_ID('dbo.fs_file_text') IS NULL
CREATE TABLE dbo.fs_file_text (
    sha256            CHAR(64)      NOT NULL PRIMARY KEY REFERENCES dbo.fs_stored_file(sha256),
    method            NVARCHAR(20)  NOT NULL,             -- native | ocr | mixed
    extractor_version NVARCHAR(100) NOT NULL,
    page_count        INT           NULL,
    ocr_pages         INT           NULL,
    low_quality       BIT           NULL,
    char_count        INT           NOT NULL,
    text_hash         CHAR(64)      NOT NULL,             -- for near-duplicate lookups
    full_text         NVARCHAR(MAX) NOT NULL,
    created_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_fs_file_text_text_hash')
CREATE INDEX IX_fs_file_text_text_hash ON dbo.fs_file_text(text_hash);
GO
