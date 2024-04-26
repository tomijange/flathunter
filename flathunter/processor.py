"""Utility classes for building chains for processors"""
from functools import reduce
from typing import List

from flathunter.default_processors import AddressResolver, AutoEmailProcessor
from flathunter.default_processors import Filter
from flathunter.default_processors import LambdaProcessor
from flathunter.default_processors import CrawlExposeDetails
from flathunter.notifiers import SenderMattermost, SenderTelegram, SenderApprise, SenderSlack, SenderConsole
from flathunter.gmaps_duration_processor import GMapsDurationProcessor
from flathunter.idmaintainer import SaveAllExposesProcessor
from flathunter.abstract_processor import Processor

class ProcessorChainBuilder:
    """Builder pattern for building chains of processors"""
    processors: List[Processor]

    def __init__(self, config):
        self.processors = []
        self.config = config

    def send_messages(self, receivers=None):
        """Add processor that sends messages for exposes"""
        notifiers = self.config.notifiers()
        if 'telegram' in notifiers:
            self.processors.append(SenderTelegram(self.config, receivers=receivers))
        if 'mattermost' in notifiers:
            self.processors.append(SenderMattermost(self.config))
        if 'apprise' in notifiers:
            self.processors.append(SenderApprise(self.config))
        if 'slack' in notifiers:
            self.processors.append(SenderSlack(self.config))
        if 'console' in notifiers:
            self.processors.append(SenderConsole(self.config))
        return self
    
    def send_emails(self):
        """Add processor that sends email to contact"""
        auto_email_active = self.config.get_auto_email_active()
        if auto_email_active:
            self.processors.append(AutoEmailProcessor(self.config))
        return self

    def resolve_addresses(self):
        """Add processor that resolves addresses from expose pages"""
        self.processors.append(AddressResolver(self.config))
        return self

    def calculate_durations(self):
        """Add processor to calculate durations, if enabled"""
        durations_enabled = "google_maps_api" in self.config \
                            and self.config["google_maps_api"]["enable"]
        if durations_enabled:
            self.processors.append(GMapsDurationProcessor(self.config))
        return self

    def crawl_expose_details(self):
        """Add processor to crawl expose details"""
        self.processors.append(CrawlExposeDetails(self.config))
        return self

    def map(self, func):
        """Add processor that applies a lambda to exposes"""
        self.processors.append(LambdaProcessor(self.config, func))
        return self

    def apply_filter(self, filter_set):
        """Add processor that applies a filter to expose sequence"""
        self.processors.append(Filter(self.config, filter_set))
        return self

    def save_all_exposes(self, id_watch):
        """Add processor that saves all exposes to disk"""
        self.processors.append(SaveAllExposesProcessor(self.config, id_watch))
        return self

    def build(self):
        """Build the processor chain"""
        return ProcessorChain(self.processors)

class ProcessorChain:
    """Class to hold a chain of processors"""
    processors: List[Processor]

    def __init__(self, processors):
        self.processors = processors

    def process(self, exposes):
        """Process the sequences of exposes with the processor chain"""
        return reduce((lambda exposes, processor: processor.process_exposes(exposes)),
                      self.processors, exposes)

    @staticmethod
    def builder(config):
        """Return a new processor chain builder"""
        return ProcessorChainBuilder(config)
