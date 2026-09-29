import os
from dataclasses import dataclass, field


@dataclass
class FileStoreConfig:
    root_dir: str = "filestore_data"
    # Bump this when the OCR engine or its settings change, so old text can be
    # told apart from text produced by the new extractor.
    extractor_version: str = "ocrprocessor-v1"
    timeout: int = 60
    chunk_size: int = 64 * 1024
    mssql: dict = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "FileStoreConfig":
        return cls(
            root_dir=os.getenv("FILESTORE_ROOT", "filestore_data"),
            extractor_version=os.getenv("FILESTORE_EXTRACTOR_VERSION", "ocrprocessor-v1"),
            mssql={
                "driver": os.getenv("MSSQL_DRIVER"),
                "server": os.getenv("MSSQL_SERVER"),
                "database": os.getenv("MSSQL_DATABASE"),
                "username": os.getenv("MSSQL_USERNAME"),
                "password": os.getenv("MSSQL_PASSWORD"),
            },
        )
