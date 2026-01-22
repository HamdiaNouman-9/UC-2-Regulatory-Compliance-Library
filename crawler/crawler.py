class BaseCrawler:
    """
    Base interface for all regulator crawlers.
    Every crawler (SBP, SECP, etc.) must implement these two methods.
    """

    def get_structure(self):
        # Returns the folder structure of the regulator's regulatory section.
        raise NotImplementedError("get_structure() must be implemented")

    def get_documents(self):
        # Returns list of regulatory documents.
        raise NotImplementedError("get_documents() must be implemented")
