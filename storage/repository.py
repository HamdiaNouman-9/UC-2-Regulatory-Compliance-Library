from abc import ABC, abstractmethod
from models.models import RegulatoryDocument

class DocumentRepository(ABC):


    @abstractmethod
    def save_metadata(self, document: RegulatoryDocument, ocr_text: str, classification: str):
        pass

    def save_ocr_path_and_fields(self, *args, **kwargs):
        pass

    def log_processing(self, *args, **kwargs):
        pass
