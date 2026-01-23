from crochet import run_in_reactor
from scrapy.crawler import CrawlerRunner
from sbp_crawler.sbp_crawler.spiders.sbp_circulars_spider import SBPCircularsSpider
from sbp_crawler.sbp_crawler.spiders.sbp_regulatory_returns_spider import SBPRegulatoryReturnsSpider
from sbp_crawler.sbp_crawler.spiders.sbp_notifications_spider import SBPNotificationsSpider
import logging
from twisted.internet import defer

logger = logging.getLogger(__name__)


class SBPCrawler:
    """
    Sequential multi-spider crawler for SBP
    Runs Circulars, Notifications, and Regulatory Returns spiders one after another
    """

    def __init__(self):
        self.items = []

    @run_in_reactor
    def get_documents(self):
        """
        Run all spiders sequentially using Twisted's defer.inlineCallbacks
        Returns a deferred that completes when all spiders finish
        """
        runner = CrawlerRunner()

        @defer.inlineCallbacks
        def crawl_sequential():
            """
            Run spiders one after another
            """
            logger.info("=" * 70)
            logger.info("Starting SBP Multi-Spider Crawl (Sequential)")
            logger.info("=" * 70)

            # Spider 1: Circulars
            logger.info("\n[1/3] Running SBP Circulars Spider...")
            try:
                yield runner.crawl(SBPCircularsSpider, shared_items=self.items)
                logger.info(f"Circulars Spider completed. Total items so far: {len(self.items)}")

                # Log breakdown by regulator
                dpc_count = sum(1 for i in self.items if getattr(i, 'regulator', '') == 'DPC')
                sbp_count = sum(1 for i in self.items if getattr(i, 'regulator', '') == 'SBP')
                logger.info(f"  → SBP: {sbp_count}, DPC: {dpc_count}")

            except Exception as e:
                logger.error(f"Circulars Spider failed: {e}", exc_info=True)

            # Spider 2: Regulatory Returns
            logger.info("\n[2/3] Running SBP Regulatory Returns Spider...")
            try:
                yield runner.crawl(SBPRegulatoryReturnsSpider, shared_items=self.items)
                logger.info(f"Regulatory Returns Spider completed. Total items so far: {len(self.items)}")
            except Exception as e:
                logger.error(f"Regulatory Returns Spider failed: {e}", exc_info=True)

            # Spider 3: Notifications
            logger.info("\n[3/3] Running SBP Notifications Spider...")
            try:
                yield runner.crawl(SBPNotificationsSpider, shared_items=self.items)
                logger.info(f"Notifications Spider completed. Total items so far: {len(self.items)}")
            except Exception as e:
                logger.error(f"Notifications Spider failed: {e}", exc_info=True)

            logger.info("\n" + "=" * 70)
            logger.info(f"All spiders completed. Total items collected: {len(self.items)}")
            logger.info("=" * 70)

        return crawl_sequential()

    def fetch_documents(self, timeout=7200):
        """
        Fetch documents from SBP website using all spiders sequentially.

        Args:
            timeout: Maximum time in seconds to wait (default: 2 hours for all spiders)
        """
        logger.info("Starting multi-spider crawl...")

        try:
            self.get_documents().wait(timeout=timeout)
            logger.info(f"\nAll crawls completed successfully.")
            logger.info(f"Total documents collected: {len(self.items)}")

            self._log_statistics()

        except TimeoutError:
            logger.warning(f"\nCrawl timed out after {timeout} seconds.")
            logger.info(f"Partial results: {len(self.items)} documents collected")

        except Exception as e:
            logger.error(f"\nCrawl failed with error: {e}", exc_info=True)
            logger.info(f"Documents collected before error: {len(self.items)}")

        return self.items

    def _log_statistics(self):
        """
        Log statistics about collected documents
        """
        if not self.items:
            logger.info("No items collected")
            return

        # Count by regulator/source
        stats = {}
        for item in self.items:
            source = getattr(item, 'source_system', 'Unknown')
            stats[source] = stats.get(source, 0) + 1

        logger.info("\nCollection Statistics:")
        for source, count in stats.items():
            logger.info(f"  {source}: {count} documents")

        # Also show regulator breakdown
        regulator_stats = {}
        for item in self.items:
            regulator = getattr(item, 'regulator', 'Unknown')
            regulator_stats[regulator] = regulator_stats.get(regulator, 0) + 1

        logger.info("\nRegulator Breakdown:")
        for regulator, count in regulator_stats.items():
            logger.info(f"  {regulator}: {count} documents")